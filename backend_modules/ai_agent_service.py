from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, Union

from .ai_tool_registry import AIToolDefinition, AIToolRegistry


CommandReply = Union[str, dict[str, Any]]
PrepareQuestion = Callable[[str], tuple[str, str]]
ExecutionProposalBuilder = Callable[[str], Optional[CommandReply]]
AllowedToolSelector = Callable[[str], Iterable[str]]
SubagentSelector = Callable[[str], str]


@dataclass
class AIAgentResult:
    question: str
    handled: bool
    reply: Optional[CommandReply] = None
    tool: Optional[AIToolDefinition] = None
    source: str = ""
    subagent: str = ""


class AIAgentService:
    def __init__(
        self,
        *,
        registry: AIToolRegistry,
        prepare_question: PrepareQuestion,
        build_execution_proposal: ExecutionProposalBuilder,
        select_allowed_tools: AllowedToolSelector | None = None,
        select_subagent: SubagentSelector | None = None,
    ) -> None:
        self._registry = registry
        self._prepare_question = prepare_question
        self._build_execution_proposal = build_execution_proposal
        self._select_allowed_tools = select_allowed_tools
        self._select_subagent = select_subagent

    @property
    def registry(self) -> AIToolRegistry:
        return self._registry

    def format_tool_registry_context(self) -> str:
        return self._registry.format_context()

    def prepare_and_dispatch(self, question: str) -> AIAgentResult:
        prepared_question, immediate_reply = self._prepare_question(str(question or "").strip())
        subagent = str(self._select_subagent(prepared_question) if self._select_subagent else "").strip()
        if immediate_reply:
            return AIAgentResult(
                question=prepared_question,
                handled=True,
                reply=immediate_reply,
                source="route",
                subagent=subagent,
            )

        execution_reply = self._build_execution_proposal(prepared_question)
        if execution_reply:
            return AIAgentResult(
                question=prepared_question,
                handled=True,
                reply=execution_reply,
                source="confirm",
                subagent=subagent,
            )

        allowed_tools = list(self._select_allowed_tools(prepared_question) if self._select_allowed_tools else [])
        dispatched = self._registry.dispatch(prepared_question, allowed_names=allowed_tools or None)
        if dispatched:
            tool, reply = dispatched
            return AIAgentResult(
                question=prepared_question,
                handled=True,
                reply=reply,
                tool=tool,
                source="tool",
                subagent=subagent,
            )
        return AIAgentResult(question=prepared_question, handled=False, subagent=subagent)
