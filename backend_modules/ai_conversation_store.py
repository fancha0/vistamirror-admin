from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import json
import pathlib
import threading
from typing import Any


class AiConversationStore:
    def __init__(self, path: pathlib.Path, *, recent_limit: int = 80) -> None:
        self.path = path
        self.recent_limit = max(20, int(recent_limit))
        self._lock = threading.RLock()
        self._sessions: dict[str, dict[str, Any]] = {}
        self._load()

    def get(self, key: str) -> dict[str, Any]:
        safe_key = str(key or "").strip()
        if not safe_key:
            return {}
        with self._lock:
            return deepcopy(self._sessions.get(safe_key) or {})

    def remember(self, key: str, *, question: str, answer: str) -> None:
        safe_key = str(key or "").strip()
        if not safe_key:
            return
        with self._lock:
            session = self._sessions.setdefault(safe_key, self._empty_session())
            rows = session.setdefault("recent", [])
            rows.append(
                {
                    "time": datetime.now().isoformat(timespec="minutes"),
                    "user": str(question or "").strip()[:2000],
                    "assistant": str(answer or "").strip()[:6000],
                }
            )
            overflow = max(0, len(rows) - self.recent_limit)
            if overflow:
                removed = rows[:overflow]
                del rows[:overflow]
                session["summary"] = self._extend_summary(str(session.get("summary") or ""), removed)
            session["updatedAt"] = datetime.now().isoformat(timespec="seconds")
            self._save_locked()

    def set_active_media(self, key: str, media: dict[str, Any]) -> None:
        safe_key = str(key or "").strip()
        if not safe_key:
            return
        allowed = {
            "title",
            "year",
            "type",
            "embySeriesId",
            "tmdbId",
            "latestEpisode",
            "actualEpisodeCount",
            "expectedEpisodeCount",
            "missingEpisodeCount",
            "seasonCount",
        }
        clean = {field: media.get(field) for field in allowed if media.get(field) not in (None, "")}
        with self._lock:
            session = self._sessions.setdefault(safe_key, self._empty_session())
            session["activeMedia"] = clean
            session["updatedAt"] = datetime.now().isoformat(timespec="seconds")
            self._save_locked()

    def clear(self, key: str) -> bool:
        safe_key = str(key or "").strip()
        if not safe_key:
            return False
        with self._lock:
            existed = safe_key in self._sessions
            self._sessions.pop(safe_key, None)
            self._save_locked()
            return existed

    @staticmethod
    def _empty_session() -> dict[str, Any]:
        return {"summary": "", "recent": [], "activeMedia": {}, "updatedAt": ""}

    @staticmethod
    def _extend_summary(existing: str, removed: list[dict[str, Any]]) -> str:
        lines = [line for line in str(existing or "").splitlines() if line.strip()]
        for row in removed:
            question = str(row.get("user") or "").strip().replace("\n", " ")[:180]
            answer = str(row.get("assistant") or "").strip().replace("\n", " ")[:260]
            if question or answer:
                lines.append(f"用户：{question}｜助手：{answer}")
        return "\n".join(lines[-120:])

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return
        sessions = payload.get("sessions") if isinstance(payload, dict) else None
        if isinstance(sessions, dict):
            self._sessions = {str(key): value for key, value in sessions.items() if isinstance(value, dict)}

    def _save_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        payload = {"version": 1, "sessions": self._sessions}
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(self.path)
