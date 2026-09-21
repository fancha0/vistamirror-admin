"""Persistent public TMDB metadata only; never stores tokens or missing verdicts."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import time
from typing import Any


class MissingMetadataCache:
    def __init__(self, path: Path, *, clock=time.time) -> None:
        self.path = path
        self.clock = clock
        self.available = False
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as db:
                db.execute('CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, expires REAL NOT NULL, payload TEXT NOT NULL)')
                db.execute('DELETE FROM metadata WHERE expires <= ?', (self.clock(),))
                db.execute('DELETE FROM metadata WHERE key IN (SELECT key FROM metadata ORDER BY expires DESC LIMIT -1 OFFSET 20000)')
            self.available = True
        except (OSError, sqlite3.Error):
            pass  # A broken/read-only cache must not prevent a fresh scan.

    def _connect(self):
        return sqlite3.connect(str(self.path), timeout=3)

    def get(self, key: str) -> dict[str, Any] | None:
        if not self.available:
            return None
        try:
            with self._connect() as db:
                row = db.execute('SELECT payload FROM metadata WHERE key = ? AND expires > ?', (key, self.clock())).fetchone()
            if row:
                value = json.loads(row[0])
                return value if isinstance(value, dict) else None
        except (sqlite3.Error, ValueError):
            pass
        return None

    def put(self, key: str, payload: dict[str, Any], ttl: int) -> bool:
        if not self.available:
            return False
        try:
            with self._connect() as db:
                db.execute('INSERT OR REPLACE INTO metadata VALUES (?, ?, ?)', (key, self.clock() + ttl, json.dumps(payload, ensure_ascii=False)))
            return True
        except (sqlite3.Error, ValueError):
            return False
