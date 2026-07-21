from __future__ import annotations

"""Shared strict missing-episode inspector.

The Telegram AI workflow already owns the safest implementation of missing
episode comparison: confirmed TMDB identity, aired episodes only, then a
per-season Emby diff.  The dashboard uses this small bridge instead of
maintaining another approximation of that algorithm.
"""

from typing import Any

from .ai_media_service_adapter import AIMediaServiceAdapter
from .ai_missing_episode_support import MissingEpisodeResult


class _StrictMissingEpisodeAdapter(AIMediaServiceAdapter):
    """Supplies a MediaIdentityService to the existing canonical AI algorithm."""

    def __init__(self, identity_service: Any) -> None:
        # The inherited calculation only needs media_identity_service() and its
        # own pure mapping helpers; a Telegram runtime is deliberately absent.
        self._identity_service = identity_service

    def media_identity_service(self) -> Any:
        return self._identity_service


class MissingEpisodeInspector:
    """Reusable strict inspector for AI and non-AI entry points."""

    def __init__(self, *, identity_service: Any) -> None:
        self._adapter = _StrictMissingEpisodeAdapter(identity_service)

    def inspect(self, *, identity: dict[str, Any], server: str = "emby") -> MissingEpisodeResult:
        return self._adapter.build_missing_episode_result(identity=identity, server=server)
