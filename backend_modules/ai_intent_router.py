from __future__ import annotations

import json
import re
from typing import Any, Callable

from .ai_missing_episode_support import is_missing_episode_meta_question


ROUTE_INTENTS = {
    "media_missing_episodes",
    "media_episode_progress",
    "media_detail",
    "media_search",
    "media_category_list",
    "playback_query",
    "task_query",
    "execution_request",
    "media_correction",
    "general_chat",
}


class AiIntentRouter:
    def __init__(self, completion: Callable[..., str]) -> None:
        self.completion = completion

    def route(
        self,
        question: str,
        *,
        config: dict[str, Any],
        active_media: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        text = str(question or "").strip()
        fallback = self.fallback_route(text)
        if not text:
            return fallback
        router_config = dict(config if isinstance(config, dict) else {})
        router_config["temperature"] = 0
        router_config["maxTokens"] = 300
        active_title = str((active_media or {}).get("title") or "").strip()
        messages = [
            {
                "role": "system",
                "content": (
                    "你是 Vistamirror 的中文意图路由器，只输出一个 JSON 对象，禁止解释和 Markdown。"
                    "字段固定为 intent, mediaTitle, mediaType, useActiveMedia, isCorrection, confidence。"
                    "intent 只能是 media_missing_episodes, media_episode_progress, media_detail, media_search, "
                    "media_category_list, playback_query, task_query, execution_request, media_correction, general_chat。"
                    "mediaType 只能是 tv, movie 或空字符串。confidence 为 0 到 1。"
                    "提取影视名称时去掉‘帮我、看一下、我的库里、的缺失集’等查询词，但不得随意删除正式片名中的‘我/我的’，"
                    "例如《我的阿勒泰》《我推的孩子》《我是刑警》必须保持完整。"
                    "用户在责骂或纠正时，只提取‘我说的是、查的是、不是A是B’之后真正的片名。"
                    "省略片名并使用‘它、这个、这部、缺失的集’时 useActiveMedia=true。"
                    "示例：‘看一下我仙逆的缺失集’=>"
                    '{"intent":"media_missing_episodes","mediaTitle":"仙逆","mediaType":"tv","useActiveMedia":false,"isCorrection":false,"confidence":0.96}。'
                    "示例：‘我说的是仙逆’=>"
                    '{"intent":"media_correction","mediaTitle":"仙逆","mediaType":"tv","useActiveMedia":false,"isCorrection":true,"confidence":0.99}。'
                    "示例：‘你识别错了，我说仙逆的缺失集’=>"
                    '{"intent":"media_missing_episodes","mediaTitle":"仙逆","mediaType":"tv","useActiveMedia":false,"isCorrection":true,"confidence":0.99}。'
                    "示例：‘它缺哪几集’=>"
                    '{"intent":"media_missing_episodes","mediaTitle":"","mediaType":"tv","useActiveMedia":true,"isCorrection":false,"confidence":0.99}。'
                    "示例：‘庆余年更新到哪集’=>"
                    '{"intent":"media_episode_progress","mediaTitle":"庆余年","mediaType":"tv","useActiveMedia":false,"isCorrection":false,"confidence":0.98}。'
                ),
            },
            {
                "role": "user",
                "content": f"当前会话作品：{active_title or '无'}\n用户消息：{text}",
            },
        ]
        try:
            raw = self.completion(config=router_config, messages=messages, timeout_seconds=20)
            payload = self._parse_json(raw)
            route = self._normalize_route(payload)
            route["source"] = "llm"
            return route
        except Exception as err:
            fallback["routerError"] = type(err).__name__
            return fallback

    @classmethod
    def fallback_route(cls, question: str) -> dict[str, Any]:
        text = str(question or "").strip()
        correction_title = cls.extract_correction_title(text)
        missing = bool(re.search(r"缺失.*集|缺少.*集|缺哪.*集|漏.*集|缺集", text)) and not is_missing_episode_meta_question(text)
        progress = bool(re.search(r"多少集|几集|更新到|最新.*集", text))
        detail = bool(re.search(r"简介|剧情|详情|演员|主演|评分", text))
        if missing:
            intent = "media_missing_episodes"
        elif correction_title:
            intent = "media_correction"
        elif progress:
            intent = "media_episode_progress"
        elif detail:
            intent = "media_detail"
        elif re.search(r"播放历史|最近.*看|谁看|播放最多", text):
            intent = "playback_query"
        elif re.search(r"任务|扫描|运行|执行|触发", text):
            intent = "execution_request" if re.search(r"运行|执行|触发|开始扫描", text) else "task_query"
        else:
            intent = "general_chat"
        use_active = bool(re.search(r"^(?:它|这个|这部|那个|那部)?\s*(?:的)?(?:缺失|缺少|缺哪|漏)|它.*(?:简介|多少集|哪集)", text))
        title = correction_title
        return {
            "intent": intent,
            "mediaTitle": title,
            "mediaType": "tv" if intent.startswith("media_") else "",
            "useActiveMedia": use_active,
            "isCorrection": bool(correction_title),
            "confidence": 0.45,
            "source": "fallback",
        }

    @staticmethod
    def extract_correction_title(text: str) -> str:
        value = str(text or "").strip()
        patterns = (
            r"不是\s*[^，。！？,]+[，,]?\s*(?:而)?是\s*[《「“\"]?(?P<title>[^》」”\"，。！？,]{2,40})",
            r"(?:我说的是|我是说|我说|查的是|要查的是|想查的是)\s*[《「“\"]?(?P<title>[^》」”\"，。！？,]{2,40})",
        )
        for pattern in patterns:
            match = re.search(pattern, value, flags=re.IGNORECASE)
            if not match:
                continue
            title = str(match.group("title") or "").strip()
            title = re.sub(r"(?:的)?(?:缺失|缺少|缺哪|漏掉|漏)(?:的|哪几|哪些)?集.*$", "", title).strip()
            title = re.sub(r"(?:的)?(?:多少集|几集|简介|剧情|详情).*$", "", title).strip()
            title = title.strip(" 《》「」“”\"'，。！？?：:；;")
            if len(title) >= 2:
                return title
        return ""

    @classmethod
    def title_candidates(cls, *, question: str, llm_title: str, rule_title: str = "") -> list[str]:
        candidates: list[str] = []
        correction = cls.extract_correction_title(question)
        for raw in (llm_title, correction, rule_title):
            title = str(raw or "").strip().strip("《》「」“”\"'，。！？?：:；;")
            if title and title not in candidates:
                candidates.append(title)
        for title in list(candidates):
            if title.startswith("我") and not title.startswith(("我的", "我们", "我是", "我叫", "我推")) and len(title) > 2:
                stripped = title[1:].strip()
                if len(stripped) >= 2 and stripped not in candidates:
                    candidates.append(stripped)
        return candidates

    @staticmethod
    def _parse_json(raw: str) -> dict[str, Any]:
        text = str(raw or "").strip()
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise ValueError("router response is not JSON")
        payload = json.loads(text[start : end + 1])
        if not isinstance(payload, dict):
            raise ValueError("router response is not an object")
        return payload

    @staticmethod
    def _normalize_route(payload: dict[str, Any]) -> dict[str, Any]:
        intent = str(payload.get("intent") or "general_chat").strip()
        if intent not in ROUTE_INTENTS:
            intent = "general_chat"
        media_type = str(payload.get("mediaType") or "").strip().lower()
        if media_type not in {"tv", "movie"}:
            media_type = ""
        try:
            confidence = max(0.0, min(1.0, float(payload.get("confidence") or 0)))
        except (TypeError, ValueError):
            confidence = 0.0
        return {
            "intent": intent,
            "mediaTitle": str(payload.get("mediaTitle") or "").strip()[:80],
            "mediaType": media_type,
            "useActiveMedia": bool(payload.get("useActiveMedia")),
            "isCorrection": bool(payload.get("isCorrection")),
            "confidence": confidence,
        }
