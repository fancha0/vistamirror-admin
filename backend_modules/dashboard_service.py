"""Read-only library trend aggregation; bounded pagination and in-memory cache."""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Any, Callable


class LibraryTrendService:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cache: dict[tuple, dict] = {}

    def get(self, base_url: str, api_key: str, offset: int, fetch: Callable[[str], Any],
            *, now: datetime | None = None, max_pages: int = 100) -> dict:
        zone = timezone(timedelta(minutes=max(-840, min(840, offset))))
        current = (now or datetime.now(timezone.utc)).astimezone(zone)
        today = current.date()
        key = (base_url, hashlib.sha256(api_key.encode()).hexdigest(), offset, str(today))
        # Single flight: dashboard polls share the same cached aggregation.
        with self._lock:
            cached = self._cache.get(key)
            if cached and time.monotonic() - cached["at"] < 300:
                return copy.deepcopy(cached["payload"])
            start = today - timedelta(days=29)
            bins = {str(start + timedelta(days=i)): {"movies": 0, "episodes": 0} for i in range(30)}
            seen = set()
            scanned = 0
            complete = False
            invalid = 0
            deadline = time.monotonic() + 25
            for page in range(max_pages):
                if page and time.monotonic() >= deadline:
                    break
                query = urllib.parse.urlencode({"Recursive": "true", "IncludeItemTypes": "Movie,Episode",
                    "SortBy": "DateCreated", "SortOrder": "Descending", "Limit": 500,
                    "StartIndex": page * 500, "Fields": "DateCreated", "EnableImages": "false",
                    "EnableUserData": "false"})
                data = fetch(f"/Items?{query}")
                if not isinstance(data, dict) or not isinstance(data.get("Items"), list):
                    raise ValueError("入库趋势返回格式不正确")
                items = data["Items"]
                new_count = 0
                crossed_start = False
                for item in items:
                    identity = item.get("Id")
                    if identity and identity in seen:
                        continue
                    if identity:
                        seen.add(identity)
                    new_count += 1
                    scanned += 1
                    try:
                        created = datetime.fromisoformat(str(item.get("DateCreated") or "").replace("Z", "+00:00"))
                        day = created.replace(tzinfo=timezone.utc).astimezone(zone).date() if created.tzinfo is None else created.astimezone(zone).date()
                    except ValueError:
                        invalid += 1
                        continue
                    if day < start:
                        crossed_start = True
                    if str(day) in bins and item.get("Type") in {"Movie", "Episode"}:
                        bins[str(day)]["movies" if item["Type"] == "Movie" else "episodes"] += 1
                total = data.get("TotalRecordCount")
                if crossed_start or len(items) < 500 or (isinstance(total, int) and (page + 1) * 500 >= total):
                    complete = True
                    break
                if not new_count:
                    break  # Upstream ignored StartIndex; disclose partial data.
            payload = {"days": [{"date": day, **counts} for day, counts in bins.items()],
                       "partial": not complete or invalid > 0, "scanned": scanned,
                       "checkedAt": current.isoformat()}
            if len(self._cache) >= 8:
                self._cache.pop(next(iter(self._cache)))
            self._cache[key] = {"at": time.monotonic(), "payload": payload}
            return copy.deepcopy(payload)
