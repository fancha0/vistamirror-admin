from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
from pathlib import Path
import secrets
import time
from typing import Any, ClassVar
from urllib.parse import quote

from .drive115_service import Drive115Service


VIDEO_EXTENSIONS = {".3gp", ".asf", ".avi", ".flv", ".m2ts", ".m4v", ".mkv", ".mov", ".mp4", ".mpeg", ".mpg", ".rmvb", ".ts", ".webm", ".wmv"}


def _bounded_int(value: Any, fallback: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = fallback
    return max(minimum, min(maximum, parsed))


def default_strm115_config() -> dict[str, Any]:
    return {
        "enabled": False,
        "sourceCid": "",
        "outputDir": "",
        "publicBaseUrl": "",
        "embyLibraryId": "",
        "syncMode": "safe_incremental",
        "requestIntervalMs": 1200,
        "maxPagesPerRun": 20,
        "scheduleEnabled": False,
        "scheduleIntervalHours": 12,
        "playbackCacheMinutes": 20,
        "signingSecret": "",
        "updatedAt": "",
    }


def normalize_strm115_config(raw: Any) -> dict[str, Any]:
    value = raw if isinstance(raw, dict) else {}
    return {
        "enabled": bool(value.get("enabled")),
        "sourceCid": str(value.get("sourceCid") or value.get("source_cid") or "").strip(),
        "outputDir": str(value.get("outputDir") or value.get("output_dir") or "").strip(),
        "publicBaseUrl": str(value.get("publicBaseUrl") or value.get("public_base_url") or "").rstrip("/"),
        "embyLibraryId": str(value.get("embyLibraryId") or value.get("emby_library_id") or "").strip(),
        "syncMode": str(value.get("syncMode") or "safe_incremental").strip() if str(value.get("syncMode") or "safe_incremental").strip() in {"safe_incremental", "full"} else "safe_incremental",
        "requestIntervalMs": _bounded_int(value.get("requestIntervalMs"), 1200, 300, 10000),
        "maxPagesPerRun": _bounded_int(value.get("maxPagesPerRun"), 20, 1, 500),
        "scheduleEnabled": bool(value.get("scheduleEnabled")),
        "scheduleIntervalHours": _bounded_int(value.get("scheduleIntervalHours"), 12, 1, 168),
        "playbackCacheMinutes": _bounded_int(value.get("playbackCacheMinutes"), 20, 0, 120),
        "signingSecret": str(value.get("signingSecret") or value.get("signing_secret") or "").strip(),
        "updatedAt": str(value.get("updatedAt") or "").strip(),
    }


def merge_strm115_config(current: Any, incoming: Any) -> dict[str, Any]:
    base = normalize_strm115_config(current)
    source = incoming if isinstance(incoming, dict) else {}
    for key in ("enabled", "sourceCid", "outputDir", "publicBaseUrl", "embyLibraryId", "syncMode", "requestIntervalMs", "maxPagesPerRun", "scheduleEnabled", "scheduleIntervalHours", "playbackCacheMinutes"):
        if key in source:
            base[key] = source[key]
    supplied_secret = str(source.get("signingSecret") or "").strip()
    if supplied_secret:
        base["signingSecret"] = supplied_secret
    if not base["signingSecret"]:
        base["signingSecret"] = secrets.token_urlsafe(32)
    base["updatedAt"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return normalize_strm115_config(base)


def public_strm115_config(raw: Any) -> dict[str, Any]:
    value = normalize_strm115_config(raw)
    value.pop("signingSecret", None)
    value["hasSigningSecret"] = bool(str(normalize_strm115_config(raw).get("signingSecret") or ""))
    return value


@dataclass
class Strm115Service:
    config: dict[str, Any]
    index_path: Path
    drive: Drive115Service
    _playback_cache: ClassVar[dict[str, dict[str, Any]]] = {}

    def __post_init__(self) -> None:
        self.config = normalize_strm115_config(self.config)
        self.index_path = Path(self.index_path)

    def validate(self) -> None:
        if not self.config.get("enabled"):
            raise RuntimeError("115 STRM 尚未启用。")
        if not self.config.get("sourceCid"):
            raise RuntimeError("请填写要同步的 115 源目录 CID。")
        output = Path(str(self.config.get("outputDir") or "")).expanduser()
        if not output.is_absolute() or output == Path("/"):
            raise RuntimeError("STRM 输出目录必须是非根目录的绝对路径。")
        if not str(self.config.get("publicBaseUrl") or "").startswith(("http://", "https://")):
            raise RuntimeError("请填写 Emby 可访问的 STRM 公网地址。")
        if not self.config.get("signingSecret"):
            raise RuntimeError("STRM 签名密钥缺失，请重新保存配置。")

    def status(self) -> dict[str, Any]:
        index = self._read_index()
        files = index.get("files") if isinstance(index.get("files"), dict) else {}
        return {
            "fileCount": len(files),
            "lastSyncedAt": str(index.get("lastSyncedAt") or ""),
            "lastSummary": index.get("lastSummary") if isinstance(index.get("lastSummary"), dict) else {},
            "syncState": index.get("syncState") if isinstance(index.get("syncState"), dict) else {},
            "orphanCount": len(index.get("orphans") if isinstance(index.get("orphans"), list) else []),
        }

    def sync(self, *, dry_run: bool = False, mode: str = "safe_incremental") -> dict[str, Any]:
        self.validate()
        mode = str(mode or "safe_incremental").strip().lower()
        if mode not in {"safe_incremental", "full", "quick_verify"}:
            raise RuntimeError("未知的 STRM 同步模式。")
        if mode == "quick_verify":
            return self._quick_verify()
        output_root = Path(str(self.config.get("outputDir") or "")).expanduser().resolve()
        index = self._read_index()
        old_files = index.get("files") if isinstance(index.get("files"), dict) else {}
        state = index.get("syncState") if isinstance(index.get("syncState"), dict) else {}
        if not state.get("active") or str(state.get("sourceCid") or "") != str(self.config["sourceCid"]):
            state = {"active": True, "sourceCid": str(self.config["sourceCid"]), "startedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"), "pending": [{"cid": str(self.config["sourceCid"]), "prefix": "", "offset": 0}], "seenFileIds": [], "pages": 0}
        pending = [row for row in state.get("pending", []) if isinstance(row, dict)]
        seen_ids = {str(value) for value in state.get("seenFileIds", []) if str(value)}
        created = updated = unchanged = failed = 0
        errors: list[dict[str, str]] = []
        scanned = videos = pages = 0
        request_interval = int(self.config.get("requestIntervalMs") or 1200) / 1000
        page_budget = int(self.config.get("maxPagesPerRun") or 20)
        while pending and pages < page_budget:
            cursor = pending.pop()
            cid, prefix, offset = str(cursor.get("cid") or ""), str(cursor.get("prefix") or ""), int(cursor.get("offset") or 0)
            if not cid:
                continue
            page = self.drive.list_directory_page(cid, offset=offset)
            pages += 1
            for row in page.get("items", []):
                if not isinstance(row, dict):
                    continue
                name = str(row.get("name") or "")
                path = f"{prefix}/{name}" if prefix else name
                if row.get("isDir"):
                    pending.append({"cid": str(row.get("id") or ""), "prefix": path, "offset": 0})
                    continue
                scanned += 1
                if Path(name).suffix.lower() not in VIDEO_EXTENSIONS:
                    continue
                videos += 1
                file_id = str(row.get("id") or "").strip()
                relative_path = self._safe_relative_path(path)
                if not file_id or not relative_path:
                    failed += 1
                    continue
                seen_ids.add(file_id)
                destination = (output_root / relative_path).with_suffix(".strm")
                record = {"id": file_id, "name": name, "path": str(relative_path), "pickCode": str(row.get("pickCode") or ""), "sha1": str(row.get("sha1") or ""), "size": int(row.get("size") or 0), "strmPath": str(destination), "updatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds")}
                content = self.signed_stream_url(file_id, suffix=Path(name).suffix)
                previous = old_files.get(file_id) if isinstance(old_files.get(file_id), dict) else {}
                try:
                    if previous.get("sha1") == record["sha1"] and previous.get("path") == record["path"] and destination.exists() and destination.read_text(encoding="utf-8").strip() == content:
                        unchanged += 1
                    elif dry_run:
                        updated += bool(previous)
                        created += not bool(previous)
                    else:
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        temporary = destination.with_suffix(".strm.tmp")
                        temporary.write_text(content + "\n", encoding="utf-8")
                        temporary.replace(destination)
                        updated += bool(previous)
                        created += not bool(previous)
                    old_files[file_id] = record
                except OSError as exc:
                    failed += 1
                    errors.append({"path": str(relative_path), "error": str(exc)[:180]})
            if page.get("hasMore"):
                pending.append({"cid": cid, "prefix": prefix, "offset": int(page.get("nextOffset") or offset)})
            if pending and request_interval:
                time.sleep(request_interval)
        complete = not pending
        removed = 0
        orphans = index.get("orphans") if isinstance(index.get("orphans"), list) else []
        if complete:
            removed_ids = set(old_files) - seen_ids
            removed = len(removed_ids)
            for file_id in removed_ids:
                previous = old_files.pop(file_id, None)
                if isinstance(previous, dict):
                    orphans.append({**previous, "detectedAt": datetime.now(timezone.utc).isoformat(timespec="seconds")})
        summary = {"mode": mode, "scanned": scanned, "videos": videos, "created": created, "updated": updated, "unchanged": unchanged, "removed": removed, "failed": failed, "dryRun": bool(dry_run), "pages": pages, "complete": complete, "remainingDirectories": len(pending)}
        if not dry_run:
            next_state = {} if complete else {"active": True, "sourceCid": str(self.config["sourceCid"]), "startedAt": state.get("startedAt"), "pending": pending, "seenFileIds": sorted(seen_ids), "pages": int(state.get("pages") or 0) + pages}
            self._write_index({"files": old_files, "orphans": orphans[-10000:], "syncState": next_state, "lastSyncedAt": datetime.now(timezone.utc).isoformat(timespec="seconds") if complete else str(index.get("lastSyncedAt") or ""), "lastSummary": summary})
        return {"ok": failed == 0, "summary": summary, "errors": errors[:20]}

    def _quick_verify(self) -> dict[str, Any]:
        self.validate()
        page = self.drive.list_directory_page(str(self.config["sourceCid"]), offset=0)
        index = self._read_index()
        return {"ok": True, "summary": {"mode": "quick_verify", "rootItems": len(page.get("items") or []), "knownVideos": len(index.get("files") if isinstance(index.get("files"), dict) else {}), "hasMore": bool(page.get("hasMore")), "complete": True, "dryRun": True}}

    def cleanup_orphans(self, *, confirm: bool = False) -> dict[str, Any]:
        self.validate()
        index = self._read_index()
        candidates = index.get("orphans") if isinstance(index.get("orphans"), list) else []
        output_root = Path(str(self.config.get("outputDir") or "")).expanduser().resolve()
        safe: list[dict[str, Any]] = []
        for item in candidates:
            if not isinstance(item, dict):
                continue
            try:
                target = Path(str(item.get("strmPath") or "")).resolve()
                target.relative_to(output_root)
            except (OSError, ValueError):
                continue
            safe.append(item)
        if not confirm:
            return {"ok": True, "preview": True, "count": len(safe), "items": [{"path": str(item.get("path") or ""), "strmPath": str(item.get("strmPath") or "")} for item in safe[:100]]}
        removed = failed = 0
        retained: list[dict[str, Any]] = []
        for item in safe:
            try:
                target = Path(str(item.get("strmPath") or "")).resolve()
                if target.exists():
                    target.unlink()
                removed += 1
            except OSError:
                failed += 1
                retained.append(item)
        self._write_index({**index, "orphans": retained})
        return {"ok": failed == 0, "preview": False, "count": len(safe), "removed": removed, "failed": failed}

    def resolve_playback_url(self, record: dict[str, Any]) -> tuple[str, bool]:
        pick_code = str(record.get("pickCode") or "").strip()
        if not pick_code:
            raise RuntimeError("STRM 索引缺少 115 pick_code，请重新同步。")
        now = time.time()
        cached = self._playback_cache.get(pick_code)
        if isinstance(cached, dict) and float(cached.get("expiresAt") or 0) > now and str(cached.get("url") or ""):
            return str(cached["url"]), True
        url = self.drive.resolve_download_url(pick_code)
        ttl = int(self.config.get("playbackCacheMinutes") or 0) * 60
        if ttl:
            self._playback_cache[pick_code] = {"url": url, "expiresAt": now + ttl}
        return url, False

    def signed_stream_url(self, file_id: str, *, suffix: str = ".mkv", ttl_seconds: int = 0) -> str:
        # STRM files must remain usable after a server restart and must not require
        # periodic rewrites. exp=0 is a stable HMAC-protected URL; rotating the
        # server-only secret revokes every generated STRM URL immediately.
        expires = 0 if int(ttl_seconds) <= 0 else int(time.time()) + max(300, int(ttl_seconds))
        signature = self._sign(str(file_id), expires)
        safe_suffix = suffix.lower() if suffix.lower() in VIDEO_EXTENSIONS else ".mkv"
        return f"{self.config['publicBaseUrl']}/d/{quote(str(file_id), safe='')}{safe_suffix}?exp={expires}&sig={signature}"

    def resolve_file(self, file_id: str, *, expires: str, signature: str) -> dict[str, Any]:
        try:
            expiry = int(expires)
        except (TypeError, ValueError):
            raise RuntimeError("STRM 链接缺少有效期。")
        if expiry != 0 and expiry < int(time.time()):
            raise RuntimeError("STRM 链接已过期，请重新同步生成。")
        expected = self._sign(str(file_id), expiry)
        if not hmac.compare_digest(expected, str(signature or "")):
            raise RuntimeError("STRM 链接签名无效。")
        record = (self._read_index().get("files") or {}).get(str(file_id))
        if not isinstance(record, dict):
            raise RuntimeError("未找到对应的 115 STRM 文件记录。")
        return record

    def _sign(self, file_id: str, expires: int) -> str:
        payload = f"115:{file_id}:{expires}".encode("utf-8")
        return hmac.new(str(self.config["signingSecret"]).encode("utf-8"), payload, hashlib.sha256).hexdigest()

    @staticmethod
    def _safe_relative_path(raw: str) -> Path | None:
        parts = [part for part in Path(raw).parts if part not in {"", ".", "..", "/"}]
        return Path(*parts) if parts else None

    def _read_index(self) -> dict[str, Any]:
        try:
            data = json.loads(self.index_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"files": {}}
        return data if isinstance(data, dict) else {"files": {}}

    def _write_index(self, value: dict[str, Any]) -> None:
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.index_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(self.index_path)
