"""Bounded reads and redaction for the STRM live log console."""
import json
import re
from pathlib import Path
from typing import Any

_SECRET = re.compile(r"cookie|token|secret|password|authorization|api.?key|pick.?code|signature|^sig$", re.I)
_URL = re.compile(r"https?://[^\s<>\"']+", re.I)


def sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): "[REDACTED]" if _SECRET.search(str(k)) else sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize(v) for v in value]
    if isinstance(value, str):
        # Error messages may embed temporary CDN URLs or signed playback paths.
        value = _URL.sub(lambda m: m[0].split("?")[0].split("#")[0] + ("?[REDACTED]" if "?" in m[0] else ""), value)
        value = re.sub(r"(?i)(/d/)[^\s?]+", r"\1[REDACTED]", value)
        value = re.sub(r"(?i)\b(cookie|authorization)\s*[:=]\s*[^\r\n]+", r"\1=[REDACTED]", value)
        return re.sub(r"(?i)\b(sig|token|secret|password|api_key)\s*[:=]\s*[^\s&;,]+", r"\1=[REDACTED]", value)
    return value


def recent_events(path: Path, limit: int = 500) -> dict:
    """Read only a bounded tail, newest first; tolerate an incomplete final line."""
    limit = max(1, min(1000, limit))
    events = []
    budget = 4 * 1024 * 1024
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            position = handle.tell()
            start = max(0, position - budget)
            carry = b""
            while position > start and len(events) < limit:
                size = min(65536, position - start)
                position -= size
                handle.seek(position)
                lines = (handle.read(size) + carry).split(b"\n")
                carry = lines.pop(0)
                if position == 0:
                    lines.insert(0, carry)
                    carry = b""
                for line in reversed(lines):
                    try:
                        event = json.loads(line)
                    except (ValueError, UnicodeDecodeError):
                        continue
                    if isinstance(event, dict) and event.get("module") == "strm115":
                        events.append(sanitize(event))
                        if len(events) == limit:
                            break
            return {"events": events, "returned": len(events), "limited": len(events) == limit or start > 0}
    except FileNotFoundError:
        return {"events": [], "returned": 0, "limited": False}
