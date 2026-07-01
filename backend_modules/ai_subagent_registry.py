from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable, Optional


@dataclass(frozen=True)
class AISubagentDefinition:
    name: str
    description: str
    tool_names: tuple[str, ...]
    instruction: str = ""


class AISubagentRegistry:
    def __init__(self, definitions: Optional[Iterable[AISubagentDefinition]] = None) -> None:
        self._definitions: list[AISubagentDefinition] = list(definitions or [])

    def register(self, *, name: str, description: str, tool_names: Iterable[str], instruction: str = "") -> None:
        self._definitions.append(
            AISubagentDefinition(
                name=str(name or "").strip(),
                description=str(description or "").strip(),
                tool_names=tuple(str(tool or "").strip() for tool in tool_names if str(tool or "").strip()),
                instruction=str(instruction or "").strip(),
            )
        )

    def definitions(self) -> list[AISubagentDefinition]:
        return list(self._definitions)

    def get(self, name: str) -> AISubagentDefinition | None:
        target = str(name or "").strip()
        for definition in self._definitions:
            if definition.name == target:
                return definition
        return None

    def pick(self, question: str) -> AISubagentDefinition | None:
        text = str(question or "").strip()
        if not text:
            return None
        lowered = text.lower()
        if self._matches_resource_operator(text, lowered):
            return self.get("resource-operator")
        if self._matches_playback_analyst(text, lowered):
            return self.get("playback-analyst")
        if self._matches_media_librarian(text, lowered):
            return self.get("media-librarian")
        return self.get("media-librarian") or (self._definitions[0] if self._definitions else None)

    @staticmethod
    def _matches_media_librarian(text: str, lowered: str) -> bool:
        return bool(
            re.search(
                r"媒体库|库里|多少集|更新到|缺失|漏集|简介|剧情|演员|主演|评分|详情|有没有|存在|tmdb|剧名|电影|剧集|动漫",
                text,
                flags=re.IGNORECASE,
            )
            or "series" in lowered
            or "episode" in lowered
            or "movie" in lowered
        )

    @staticmethod
    def _matches_playback_analyst(text: str, lowered: str) -> bool:
        return bool(re.search(r"播放历史|最近.*看|看了什么|播放记录|播放最多|观看历史", text, flags=re.IGNORECASE) or "playback" in lowered)

    @staticmethod
    def _matches_resource_operator(text: str, lowered: str) -> bool:
        return bool(
            re.search(r"115|转存|分享链接|影巢|hdhive|解锁|资源搜索", text, flags=re.IGNORECASE)
            or "115cdn" in lowered
            or "anxia.com" in lowered
            or "115.com/s/" in lowered
        )
