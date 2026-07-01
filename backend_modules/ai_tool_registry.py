from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional, Union

if TYPE_CHECKING:
    from .ai_tool_base import AIToolBase


CommandReply = Union[str, dict[str, Any]]
QuestionPredicate = Callable[[str], bool]
QuestionHandler = Callable[[str], CommandReply]


@dataclass(frozen=True)
class AIToolDefinition:
    name: str
    kind: str
    description: str
    schema: dict[str, Any] | None = None


@dataclass
class AIToolBinding:
    definition: AIToolDefinition
    tool: "AIToolBase | None" = None
    predicate: Optional[QuestionPredicate] = None
    handler: Optional[QuestionHandler] = None


class AIToolRegistry:
    def __init__(self, bindings: Optional[Iterable[AIToolBinding]] = None) -> None:
        self._bindings: list[AIToolBinding] = list(bindings or [])

    def register(
        self,
        *,
        name: str,
        kind: str,
        description: str,
        schema: dict[str, Any] | None = None,
        predicate: Optional[QuestionPredicate] = None,
        handler: Optional[QuestionHandler] = None,
    ) -> None:
        self._bindings.append(
            AIToolBinding(
                definition=AIToolDefinition(name=name, kind=kind, description=description, schema=schema),
                predicate=predicate,
                handler=handler,
            )
        )

    def register_tool(
        self,
        tool: "AIToolBase",
        *,
        predicate: Optional[QuestionPredicate] = None,
    ) -> None:
        self._bindings.append(
            AIToolBinding(
                definition=AIToolDefinition(
                    name=tool.name,
                    kind=str(getattr(tool, "kind", "read") or "read"),
                    description=tool.description,
                    schema=tool.schema,
                ),
                tool=tool,
                predicate=predicate,
            )
        )

    def definitions(self) -> list[AIToolDefinition]:
        return [binding.definition for binding in self._bindings]

    def get(self, name: str) -> AIToolDefinition | None:
        target = str(name or "").strip()
        for binding in self._bindings:
            if binding.definition.name == target:
                return binding.definition
        return None

    def get_tool(self, name: str) -> "AIToolBase | None":
        target = str(name or "").strip()
        for binding in self._bindings:
            if binding.definition.name == target:
                return binding.tool
        return None

    def by_kind(self, kind: str) -> list[AIToolDefinition]:
        target = str(kind or "").strip()
        return [binding.definition for binding in self._bindings if binding.definition.kind == target]

    def dispatch(
        self,
        question: str,
        *,
        allowed_names: Iterable[str] | None = None,
    ) -> tuple[AIToolDefinition, CommandReply] | None:
        text = str(question or "").strip()
        allowed = {str(name or "").strip() for name in allowed_names or [] if str(name or "").strip()}
        for binding in self._bindings:
            if allowed and binding.definition.name not in allowed:
                continue
            if binding.predicate is None:
                continue
            try:
                matched = bool(binding.predicate(text))
            except Exception:
                matched = False
            if not matched:
                continue
            if binding.tool is not None:
                reply = binding.tool.invoke(text)
            elif binding.handler is not None:
                reply = binding.handler(text)
            else:
                continue
            if reply:
                return binding.definition, reply
        return None

    def format_context(self) -> str:
        readable = [tool.name for tool in self.by_kind("read")]
        confirm = [tool.name for tool in self.by_kind("confirm")]
        return "\n".join(
            [
                "AI 工具注册表：",
                "- 只读工具：" + ("、".join(readable) if readable else "暂无"),
                "- 需确认工具：" + ("、".join(confirm) if confirm else "暂无"),
                "- 规则：查询类优先走工具；执行类必须让用户点击确认；敏感字段只说明已配置/未配置。",
            ]
        )
