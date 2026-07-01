from __future__ import annotations

import time
from typing import TYPE_CHECKING

from .ai_assistant import chat_completion, stream_chat_completion
from .ai_host_adapter import AIHostAdapter
from .ai_runtime_service import AIRuntimeService

if TYPE_CHECKING:
    from .telegram_commands import CommandReply, TelegramCommandService


class AIChatService:
    def __init__(
        self,
        service: "TelegramCommandService",
        *,
        question: str,
        ai_config: dict,
        conversation_key: str = "",
        chat_id: str = "",
    ) -> None:
        self.service = service
        self.host = AIHostAdapter(service)
        self.original_question = str(question or "").strip()
        self.ai_config = dict(ai_config if isinstance(ai_config, dict) else {})
        self.conversation_key = str(conversation_key or "").strip()
        self.chat_id = str(chat_id or "").strip()

    @classmethod
    def load_config(cls, service: "TelegramCommandService") -> dict:
        return AIRuntimeService(service).config_service().load()

    @classmethod
    def validate_config(cls, service: "TelegramCommandService", ai_config: dict) -> "CommandReply | None":
        return AIRuntimeService(service).config_service().validate(ai_config)

    def _runtime(self, *, rich: bool) -> AIRuntimeService:
        return AIRuntimeService(
            self.service,
            conversation_key=self.conversation_key,
            chat_id=self.chat_id,
            rich=rich,
        )

    def run_non_streaming(self) -> "CommandReply":
        runtime = self._runtime(rich=False)
        replies = runtime.reply_service()
        safety_reply = runtime.support_service().build_safety_reply(self.original_question)
        if safety_reply:
            return replies.build(safety_reply, title="AI 安全限制")

        agent = runtime.build_agent(ai_config=self.ai_config)
        agent_result = agent.prepare_and_dispatch(self.original_question)
        routed_question = agent_result.question
        if agent_result.handled:
            self._remember_tool_result(agent_result=agent_result, routed_question=routed_question)
            return self._coerce_agent_reply(agent_result.reply)

        messages = runtime.context_service().build_messages(
            routed_question,
            ai_config=self.ai_config,
        )
        started = time.time()
        try:
            answer = chat_completion(config=self.ai_config, messages=messages, timeout_seconds=45)
            elapsed_ms = int((time.time() - started) * 1000)
        except Exception as err:
            if self.host.platform.is_ai_context_limit_error(err):
                try:
                    messages = self.host.platform.shrink_ai_messages(messages)
                    answer = chat_completion(config=self.ai_config, messages=messages, timeout_seconds=45)
                    elapsed_ms = int((time.time() - started) * 1000)
                except Exception as retry_err:
                    err = retry_err
                else:
                    err = None
            if err is not None:
                self.host.platform.log_project_event(
                    level="warning",
                    module="webhook",
                    action="telegram_ai_failed",
                    message="Telegram AI 问答失败。",
                    detail={"model": str(self.ai_config.get("model") or ""), "error": str(err)},
                )
                return replies.build(str(err), title="AI 问答失败")

        self._log_success(messages=messages, elapsed_ms=elapsed_ms, streaming=False)
        self._remember_answer(question=routed_question, answer=answer)
        return replies.build(self.host.platform.truncate_text(answer, 3400))

    def run_streaming(self) -> "CommandReply":
        runtime = self._runtime(rich=True)
        replies = runtime.reply_service()
        if self.host.platform.is_ai_context_status_request(self.original_question):
            return replies.build_context_status(ai_config=self.ai_config)
        if self.host.platform.is_ai_context_reset_request(self.original_question):
            return replies.build_context_reset()
        safety_reply = runtime.support_service().build_safety_reply(self.original_question)
        if safety_reply:
            return replies.build(safety_reply, title="AI 安全限制")

        agent = runtime.build_agent(ai_config=self.ai_config)
        agent_result = agent.prepare_and_dispatch(self.original_question)
        routed_question = agent_result.question
        if agent_result.handled:
            self._remember_tool_result(agent_result=agent_result, routed_question=routed_question)
            return self._coerce_agent_reply(agent_result.reply)

        messages = runtime.context_service().build_messages(
            routed_question,
            ai_config=self.ai_config,
        )
        started = time.time()
        try:
            chunks = list(stream_chat_completion(config=self.ai_config, messages=messages, timeout_seconds=60))
            answer = "".join(chunks).strip()
            if not answer:
                raise RuntimeError("AI 流式返回内容为空")
        except Exception as stream_err:
            try:
                fallback_messages = self.host.platform.shrink_ai_messages(messages) if self.host.platform.is_ai_context_limit_error(stream_err) else messages
                answer = chat_completion(config=self.ai_config, messages=fallback_messages, timeout_seconds=45)
            except Exception as err:
                self.host.platform.log_project_event(
                    level="warning",
                    module="webhook",
                    action="telegram_ai_failed",
                    message="Telegram AI 问答失败。",
                    detail={"model": str(self.ai_config.get("model") or ""), "error": str(err)},
                )
                return replies.build(f"AI 问答失败：{err}")

        elapsed_ms = int((time.time() - started) * 1000)
        self._log_success(messages=messages, elapsed_ms=elapsed_ms, streaming=True)
        self._remember_answer(question=routed_question, answer=answer)
        return replies.build(self.host.platform.truncate_text(answer, 3400))

    def _coerce_agent_reply(self, reply: object) -> "CommandReply":
        return self._runtime(rich=False).reply_service().coerce(reply)

    def _remember_tool_result(self, *, agent_result: object, routed_question: str) -> None:
        answer = ""
        if isinstance(getattr(agent_result, "reply", None), dict):
            reply = getattr(agent_result, "reply")
            answer = str(reply.get("memory_text") or reply.get("fallback_text") or "")
        else:
            answer = str(getattr(agent_result, "reply", "") or "")
        if answer:
            self._remember_answer(question=self.original_question, answer=answer)
        if getattr(agent_result, "source", "") == "tool" and getattr(agent_result, "tool", None):
            tool = getattr(agent_result, "tool")
            self.host.platform.log_project_event(
                level="info",
                module="webhook",
                action="telegram_ai_tool_handled",
                message="Telegram AI 查询已由工具直接处理。",
                detail={"tool": tool.name, "kind": tool.kind, "chatId": self.chat_id},
            )

    def _remember_answer(self, *, question: str, answer: str) -> None:
        self.host.conversations.remember_exchange(chat_id=self.chat_id, question=question, answer=answer)
        self.host.conversations.remember(self.conversation_key, question=question, answer=answer)

    def _log_success(self, *, messages: list[dict[str, str]], elapsed_ms: int, streaming: bool) -> None:
        self.host.platform.log_project_event(
            level="info",
            module="webhook",
            action="telegram_ai_success",
            message="Telegram AI 问答已返回。",
            detail={
                "model": str(self.ai_config.get("model") or ""),
                "elapsedMs": elapsed_ms,
                "streaming": bool(streaming),
                "estimatedContextTokens": sum(
                    self.host.platform.estimate_ai_tokens(str(row.get("content") or "")) for row in messages
                ),
            },
        )
