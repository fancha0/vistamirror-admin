from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import base64
import hashlib
import hmac
import json
from pathlib import Path
import secrets
import re
import time
import tempfile
import urllib.request
import urllib.error
from typing import Any, ClassVar, Callable
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
        "includeFileName": False,
        "linkFormat": "short_named",
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
        "includeFileName": bool(value.get("includeFileName")),
        "linkFormat": value.get("linkFormat") if value.get("linkFormat") in {"signed", "named", "short_named"} else ("named" if value.get("includeFileName") else "signed"),
        "playbackCacheMinutes": _bounded_int(value.get("playbackCacheMinutes"), 20, 0, 120),
        "signingSecret": str(value.get("signingSecret") or value.get("signing_secret") or "").strip(),
        "updatedAt": str(value.get("updatedAt") or "").strip(),
    }


def merge_strm115_config(current: Any, incoming: Any) -> dict[str, Any]:
    base = normalize_strm115_config(current)
    source = incoming if isinstance(incoming, dict) else {}
    for key in ("enabled", "sourceCid", "outputDir", "publicBaseUrl", "embyLibraryId", "syncMode", "requestIntervalMs", "maxPagesPerRun", "scheduleEnabled", "scheduleIntervalHours", "playbackCacheMinutes", "includeFileName", "linkFormat"):
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
    event_callback: Callable[..., None] | None = None
    _playback_cache: ClassVar[dict[str, dict[str, Any]]] = {}

    def __post_init__(self) -> None:
        self.config = normalize_strm115_config(self.config)
        self.index_path = Path(self.index_path)

    def _emit(self, action: str, message: str, *, level: str = "info", **detail: Any) -> None:
        if self.event_callback is None:
            return
        # Observability must not interrupt generation or playback.
        try:
            self.event_callback(level=level, module="strm115", action=action, message=message, detail=detail)
        except Exception:
            pass

    def _file_event(self, message: str, *, level: str = "info", **detail: Any) -> None:
        count = getattr(self, "_file_event_count", 0)
        self._file_event_count = count + 1
        if count < 500 or level == "error":
            self._emit("strm115_file", message, level=level, **detail)
        elif count == 500:
            self._emit("strm115_log_limit", "本次任务普通文件明细超过 500 条，后续仅记录错误、目录进度和汇总。")

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
        started = time.monotonic()
        self._file_event_count = 0
        self._emit("strm115_sync_start", "开始 STRM 同步。", mode=mode, dryRun=dry_run)
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
            self._emit("strm115_scan", "扫描目录。", path=prefix or "/", offset=offset, remainingDirectories=len(pending))
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
                    self._file_event("跳过非视频文件。", path=path)
                    continue
                videos += 1
                file_id = str(row.get("id") or "").strip()
                relative_path = self._safe_relative_path(path)
                if not file_id or not relative_path:
                    failed += 1
                    self._file_event("跳过无效文件记录。", level="error", path=path, error="缺少文件 ID 或有效路径")
                    continue
                seen_ids.add(file_id)
                destination = (output_root / relative_path).with_suffix(".strm")
                record = {"id": file_id, "name": name, "path": str(relative_path), "pickCode": str(row.get("pickCode") or ""), "sha1": str(row.get("sha1") or ""), "size": int(row.get("size") or 0), "strmPath": str(destination), "updatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds")}
                content = self.signed_stream_url(file_id, suffix=Path(name).suffix, name=name)
                previous = old_files.get(file_id) if isinstance(old_files.get(file_id), dict) else {}
                try:
                    if previous.get("sha1") == record["sha1"] and previous.get("path") == record["path"] and destination.exists() and destination.read_text(encoding="utf-8").strip() == content:
                        unchanged += 1
                        self._file_event("跳过：STRM 内容未变化。", path=str(relative_path))
                    elif dry_run:
                        self._file_event("预览：待更新 STRM。" if previous else "预览：待生成 STRM。", path=str(relative_path))
                        updated += bool(previous)
                        created += not bool(previous)
                    else:
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        temporary = destination.with_suffix(".strm.tmp")
                        temporary.write_text(content + "\n", encoding="utf-8")
                        temporary.replace(destination)
                        self._file_event("已更新 STRM。" if previous else "已生成 STRM。", path=str(relative_path))
                        updated += bool(previous)
                        created += not bool(previous)
                    old_files[file_id] = record
                except OSError as exc:
                    failed += 1
                    errors.append({"path": str(relative_path), "error": str(exc)[:180]})
                    self._file_event("STRM 写入失败。", level="error", path=str(relative_path), error=str(exc))
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
        summary["elapsedMs"] = round((time.monotonic() - started) * 1000)
        return {"ok": failed == 0, "summary": summary, "errors": errors[:20]}

    def rewrite_links(self) -> dict[str, Any]:
        """Rewrite only existing, indexed STRM files; never rescan the drive."""
        self.validate()
        self._file_event_count = 0
        self._emit("strm115_rewrite_start", "开始更新已生成 STRM，不扫描网盘。")
        index = self._read_index()
        root = Path(self.config["outputDir"]).expanduser().resolve()
        counts = {"updated": 0, "unchanged": 0, "missing": 0, "failed": 0}
        errors = []
        for file_id, record in (index.get("files") or {}).items():
            temporary = None
            try:
                if not isinstance(record, dict):
                    raise ValueError("索引记录无效")
                path = Path(str(record.get("strmPath") or ""))
                if not path.is_absolute() or path.is_symlink() or path.suffix.lower() != ".strm":
                    raise ValueError("索引路径不是可更新的 STRM 文件")
                target = path.resolve()
                target.relative_to(root)
                if not target.exists():
                    counts["missing"] += 1
                    self._file_event("跳过：本地 STRM 不存在。", path=str(path))
                    continue
                content = self.signed_stream_url(str(file_id), suffix=Path(str(record.get("name") or "")).suffix, name=str(record.get("name") or ""))
                if target.read_text(encoding="utf-8").strip() == content:
                    counts["unchanged"] += 1
                    self._file_event("跳过：播放地址未变化。", path=str(path))
                    continue
                with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=target.parent, prefix=".strm-rewrite-", delete=False) as handle:
                    temporary = Path(handle.name)
                    handle.write(content + "\n")
                temporary.chmod(target.stat().st_mode & 0o777)
                temporary.replace(target)
                counts["updated"] += 1
                self._file_event("已更新 STRM 播放地址。", path=str(path))
            except (OSError, ValueError, RuntimeError) as exc:
                counts["failed"] += 1
                self._file_event("更新 STRM 失败。", level="error", fileId=str(file_id), error=str(exc))
                if len(errors) < 20:
                    errors.append({"fileId": str(file_id), "error": str(exc)[:180]})
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
        # Refresh the short-token lookup while retaining the sync cursor and history.
        self._write_index(index)
        return {"ok": counts["failed"] == 0, "summary": counts, "errors": errors}

    def _short_token(self, file_id: str) -> str:
        digest = hmac.new(self.config["signingSecret"].encode(), f"115:short:v1:{file_id}".encode(), hashlib.sha256).digest()
        return "s_" + base64.urlsafe_b64encode(digest[:18]).decode().rstrip("=")

    def resolve_short_file(self, token: str) -> dict[str, Any]:
        if not self.config.get("signingSecret") or not re.fullmatch(r"s_[A-Za-z0-9_-]{24}", token):
            raise RuntimeError("STRM 短链接无效。")
        index = self._read_index()
        files = index.get("files") or {}
        lookup = index.get("shortLinks")
        file_id = lookup.get(token) if isinstance(lookup, dict) else None
        # Existing indexes are supported before the first rewrite/sync.
        if file_id is None:
            file_id = next((key for key in files if hmac.compare_digest(self._short_token(str(key)), token)), None)
        if file_id is None or not hmac.compare_digest(self._short_token(str(file_id)), token):
            raise RuntimeError("STRM 短链接无效或文件已移除。")
        record = files.get(file_id)
        if not isinstance(record, dict):
            raise RuntimeError("未找到对应的 115 STRM 文件记录。")
        return record

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

    def resolve_playback_url(self, record: dict[str, Any], *, user_agent: str = "", force: bool = False) -> tuple[str, bool]:
        pick_code = str(record.get("pickCode") or "").strip()
        if not pick_code:
            raise RuntimeError("STRM 索引缺少 115 pick_code，请重新同步。")
        started = time.monotonic()
        self._emit("strm115_resolve_start", "解析播放直链。", name=str(record.get("name") or ""))
        now = time.time()
        cache_key = hashlib.sha256(f"{self.drive.cookie}:{pick_code}:{user_agent}".encode()).hexdigest()
        cached = None if force else self._playback_cache.get(cache_key)
        if isinstance(cached, dict) and float(cached.get("expiresAt") or 0) > now and str(cached.get("url") or ""):
            self._emit("strm115_resolve_cache", "命中播放直链缓存。", name=str(record.get("name") or ""), cacheHit=True)
            return str(cached["url"]), True
        url = self.drive.resolve_download_url(pick_code, user_agent=user_agent)
        ttl = int(self.config.get("playbackCacheMinutes") or 0) * 60
        if ttl:
            if len(self._playback_cache) >= 512:
                self._playback_cache.clear()
            self._playback_cache[cache_key] = {"url": url, "expiresAt": now + ttl}
        self._emit("strm115_resolve_done", "播放直链解析完成。", name=str(record.get("name") or ""), elapsedMs=round((time.monotonic() - started) * 1000), cacheHit=False)
        return url, False

    def playback_samples(self) -> list[dict[str, str]]:
        return [{"id": str(key), "name": str(row.get("name") or key)}
                for key, row in (self._read_index().get("files") or {}).items()][:50]

    def test_playback(self, file_id: str, *, user_agent: str = "") -> dict[str, Any]:
        self.validate()
        records = self._read_index().get("files") or {}
        record = records.get(file_id)
        if not isinstance(record, dict):
            raise RuntimeError("请选择已同步的视频，或先执行同步。")
        start = time.monotonic()
        url, _ = self.resolve_playback_url(record, user_agent=user_agent, force=True)
        request = urllib.request.Request(url, headers={"User-Agent": user_agent, "Range": "bytes=0-1023"})
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                status = response.status
                content_type = str(response.headers.get("Content-Type") or "").lower()
                sample = response.read(1024)
                if status not in (200, 206) or not sample or "text/html" in content_type or "json" in content_type:
                    raise RuntimeError("直链返回的不是有效视频数据。")
                ranged = status == 206 and str(response.headers.get("Content-Range") or "").startswith("bytes 0-")
        except urllib.error.HTTPError as err:
            raise RuntimeError(f"直链已解析，但视频读取返回 HTTP {err.code}；请检查账号与播放网络。") from err
        except (urllib.error.URLError, TimeoutError) as err:
            raise RuntimeError("直链已解析，但视频数据读取失败；请检查播放网络。") from err
        return {"ok": True, "fileName": str(record.get("name") or file_id), "readBytes": len(sample),
                "rangeSupported": ranged, "elapsedMs": round((time.monotonic() - start) * 1000),
                "message": "服务器已读取视频数据；仍需在 Emby 验证起播和拖动。",
                "streamUrl": self.signed_stream_url(file_id, suffix=Path(str(record.get("name") or "")).suffix, name=str(record.get("name") or ""))}

    def signed_stream_url(self, file_id: str, *, suffix: str = ".mkv", ttl_seconds: int = 0, name: str = "") -> str:
        # STRM files must remain usable after a server restart and must not require
        # periodic rewrites. exp=0 is a stable HMAC-protected URL; rotating the
        # server-only secret revokes every generated STRM URL immediately.
        expires = 0 if int(ttl_seconds) <= 0 else int(time.time()) + max(300, int(ttl_seconds))
        signature = self._sign(str(file_id), expires)
        safe_suffix = suffix.lower() if suffix.lower() in VIDEO_EXTENSIONS else ".mkv"
        if self.config.get("linkFormat") == "short_named" and expires == 0:
            display_name = name or f"{file_id}{safe_suffix}"
            return f"{self.config['publicBaseUrl']}/d/{self._short_token(str(file_id))}{safe_suffix}?/{quote(display_name, safe='')}"
        url = f"{self.config['publicBaseUrl']}/d/{quote(str(file_id), safe='')}{safe_suffix}?exp={expires}&sig={signature}"
        if (self.config.get("linkFormat") == "named" or self.config.get("includeFileName")) and name:
            url += "&name=" + quote(name, safe="")
        return url

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
        value = {**value, "shortLinks": {self._short_token(str(key)): str(key) for key in (value.get("files") or {})}}
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.index_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(self.index_path)
