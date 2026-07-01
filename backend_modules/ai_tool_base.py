from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Union


CommandReply = Union[str, dict[str, Any]]


class AIToolBase(ABC):
    kind = "read"

    def __init__(
        self,
        *,
        name: str,
        description: str,
        schema: dict[str, Any] | None = None,
        kind: str = "read",
    ) -> None:
        self._name = str(name or "").strip()
        self._description = str(description or "").strip()
        self._schema = dict(schema or {})
        self.kind = str(kind or "read").strip() or "read"

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def schema(self) -> dict[str, Any]:
        return dict(self._schema)

    @abstractmethod
    def invoke(self, question: str) -> CommandReply:
        raise NotImplementedError
