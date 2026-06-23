from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import base64
from io import BytesIO
import json
import os
import re
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


DRIVE115_ENV_FIELD_MAP: dict[str, str] = {
    "cookie": "APP_115_COOKIE",
    "defaultCid": "APP_115_DEFAULT_CID",
}

DRIVE115_QRCODE_CLIENTS: dict[str, dict[str, str]] = {
    "qandroid": {"label": "115生活(Android端)", "app": "qandroid"},
    "android": {"label": "115网盘(Android端)", "app": "android"},
    "ios": {"label": "115网盘(iPad/iPhone端)", "app": "ios"},
    "web": {"label": "网页版", "app": "web"},
    "tv": {"label": "115网盘(Android电视端)", "app": "tv"},
}


def default_drive115_config() -> dict[str, Any]:
    return {
        "enabled": False,
        "cookie": "",
        "defaultCid": "0",
        "updatedAt": "",
    }


def normalize_drive115_config(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    return {
        "enabled": bool(source.get("enabled")),
        "cookie": str(source.get("cookie") or "").strip(),
        "defaultCid": str(source.get("defaultCid") or source.get("cid") or "0").strip() or "0",
        "updatedAt": str(source.get("updatedAt") or "").strip(),
    }


def mask_cookie(cookie: str) -> str:
    value = str(cookie or "").strip()
    if not value:
        return ""
    if len(value) <= 12:
        return "******"
    return f"{value[:6]}...{value[-6:]}"


def public_drive115_config(raw: Any) -> dict[str, Any]:
    config = normalize_drive115_config(raw)
    return {
        "enabled": bool(config.get("enabled")),
        "defaultCid": str(config.get("defaultCid") or "0"),
        "updatedAt": str(config.get("updatedAt") or ""),
        "hasCookie": bool(str(config.get("cookie") or "").strip()),
        "cookieMasked": mask_cookie(str(config.get("cookie") or "")),
    }


def admin_drive115_config(raw: Any) -> dict[str, Any]:
    config = normalize_drive115_config(raw)
    payload = public_drive115_config(config)
    payload["cookie"] = str(config.get("cookie") or "")
    return payload


def env_managed_drive115_fields() -> list[str]:
    managed: list[str] = []
    for field, env_name in DRIVE115_ENV_FIELD_MAP.items():
        if str(os.environ.get(env_name) or "").strip():
            managed.append(field)
    return managed


def apply_drive115_env_overrides(raw: Any) -> dict[str, Any]:
    config = normalize_drive115_config(raw)
    for field, env_name in DRIVE115_ENV_FIELD_MAP.items():
        value = str(os.environ.get(env_name) or "").strip()
        if value:
            config[field] = value
    return config


def merge_drive115_config_for_save(current: Any, incoming: Any) -> dict[str, Any]:
    current_config = normalize_drive115_config(current)
    source = incoming if isinstance(incoming, dict) else {}
    merged = dict(current_config)
    for key in ("enabled", "defaultCid"):
        if key in source:
            merged[key] = source.get(key)
    if "cookie" in source:
        cookie = str(source.get("cookie") or "").strip()
        if cookie and cookie != mask_cookie(str(current_config.get("cookie") or "")):
            merged["cookie"] = cookie
        elif source.get("clearCookie"):
            merged["cookie"] = ""
    if source.get("clearCookie"):
        merged["cookie"] = ""
    merged["updatedAt"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    return normalize_drive115_config(merged)


def redact_drive115_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        redacted: dict[str, Any] = {}
        for key, value in payload.items():
            if str(key).lower() in {"cookie", "cookies"}:
                redacted[key] = "***"
            else:
                redacted[key] = redact_drive115_payload(value)
        return redacted
    if isinstance(payload, list):
        return [redact_drive115_payload(item) for item in payload]
    return payload


def drive115_qrcode_clients() -> list[dict[str, str]]:
    return [{"value": key, "label": value["label"]} for key, value in DRIVE115_QRCODE_CLIENTS.items()]


def extract_115_share(raw: str, receive_code: str = "") -> dict[str, str]:
    text = str(raw or "").strip()
    code = ""
    for pattern in (
        r"115\.com/s/([A-Za-z0-9]+)",
        r"115cdn\.com/s/([A-Za-z0-9]+)",
        r"anxia\.com/s/([A-Za-z0-9]+)",
        r"share_code=([A-Za-z0-9]+)",
        r"\b(115[A-Za-z0-9]{6,}|[a-z][A-Za-z0-9]{8,})\b",
    ):
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            code = str(match.group(1) or "").strip()
            break
    pwd = str(receive_code or "").strip()
    if not pwd:
        for pattern in (
            r"(?:访问码|提取码|验证码|密码)[:：\s]*([A-Za-z0-9]{4})",
            r"[?&](?:password|pwd)=([A-Za-z0-9]{4})",
        ):
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                pwd = str(match.group(1) or "").strip()
                break
    return {"shareCode": code, "receiveCode": pwd}


def _human_size(value: Any) -> str:
    try:
        size = float(value or 0)
    except Exception:
        size = 0
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)}{unit}"
            return f"{size:.2f}{unit}"
        size /= 1024
    return "0B"


class Drive115Error(RuntimeError):
    def __init__(self, message: str, *, code: str = "", payload: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = str(code or "")
        self.payload = payload if isinstance(payload, dict) else {}

    @property
    def is_duplicate(self) -> bool:
        text = str(self).lower()
        return any(marker in text for marker in ("已经转存", "已转存", "重复接收", "无需重复接收", "already received"))


@dataclass
class Drive115Service:
    config: dict[str, Any]
    timeout: int = 20

    def __post_init__(self) -> None:
        self.config = apply_drive115_env_overrides(self.config)
        self._user_id = ""

    @property
    def cookie(self) -> str:
        return str(self.config.get("cookie") or "").strip()

    def _request(
        self,
        url: str,
        *,
        method: str = "GET",
        data: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        if not self.cookie:
            raise RuntimeError("115 Cookie 未配置，请先在 115 网盘页面保存 Cookie。")
        payload = None
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 Chrome/120 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Cookie": self.cookie,
            "Referer": "https://115.com/",
        }
        if data is not None:
            payload = urllib.parse.urlencode(data, doseq=True).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
        request = urllib.request.Request(url, data=payload, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout or self.timeout) as response:
                raw = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as err:
            body = err.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"115 接口失败（HTTP {err.code}）：{body[:180]}") from err
        except urllib.error.URLError as err:
            raise RuntimeError(f"115 网络错误：{getattr(err, 'reason', err)}") from err
        try:
            parsed = json.loads(raw)
        except Exception as err:
            raise RuntimeError(f"115 返回无法解析：{raw[:180]}") from err
        if not isinstance(parsed, dict):
            raise RuntimeError("115 返回格式异常。")
        return parsed

    def _request_public(
        self,
        url: str,
        *,
        method: str = "GET",
        data: dict[str, Any] | None = None,
        timeout: int | None = None,
        return_headers: bool = False,
    ) -> dict[str, Any] | tuple[dict[str, Any], Any]:
        payload = None
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 Chrome/120 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://115.com/",
        }
        if data is not None:
            payload = urllib.parse.urlencode(data, doseq=True).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
        request = urllib.request.Request(url, data=payload, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout or self.timeout) as response:
                raw = response.read().decode("utf-8", errors="replace")
                response_headers = response.headers
        except urllib.error.HTTPError as err:
            body = err.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"115 扫码接口失败（HTTP {err.code}）：{body[:180]}") from err
        except urllib.error.URLError as err:
            raise RuntimeError(f"115 扫码网络错误：{getattr(err, 'reason', err)}") from err
        try:
            parsed = json.loads(raw)
        except Exception as err:
            raise RuntimeError(f"115 扫码返回无法解析：{raw[:180]}") from err
        if not isinstance(parsed, dict):
            raise RuntimeError("115 扫码返回格式异常。")
        if return_headers:
            return parsed, response_headers
        return parsed

    @staticmethod
    def _ensure_success(payload: dict[str, Any], default_error: str) -> None:
        state = payload.get("state")
        errno = payload.get("errno")
        if state in (True, 1, "1", "true", "success") or payload.get("data") is not None:
            return
        message = str(payload.get("error") or payload.get("msg") or payload.get("message") or "").strip()
        raise Drive115Error(message or default_error, code=str(errno or payload.get("code") or ""), payload=payload)

    @staticmethod
    def _response_data(payload: dict[str, Any]) -> dict[str, Any]:
        return payload.get("data") if isinstance(payload.get("data"), dict) else payload

    def _get_user_id(self) -> str:
        if self._user_id:
            return self._user_id
        for url in ("https://my.115.com/?ct=ajax&ac=get_user_aq", "https://webapi.115.com/user/info"):
            try:
                payload = self._request(url)
                self._ensure_success(payload, "115 账号信息读取失败。")
            except RuntimeError:
                continue
            data = self._response_data(payload)
            user_id = str(data.get("user_id") or data.get("userId") or data.get("uid") or "").strip()
            if user_id:
                self._user_id = user_id
                return user_id
        raise RuntimeError("无法读取 115 用户 ID，请更新 Cookie 后重试。")

    @staticmethod
    def _normalize_file_name(value: Any) -> str:
        return re.sub(r"\s+", " ", str(value or "").strip()).casefold()

    @staticmethod
    def _file_meta(row: Any) -> dict[str, Any]:
        source = row if isinstance(row, dict) else {}
        size = source.get("size") if source.get("size") is not None else source.get("s")
        try:
            size_value = int(float(size or 0))
        except (TypeError, ValueError):
            size_value = 0
        return {
            "name": str(source.get("name") or source.get("n") or source.get("file_name") or "").strip(),
            "size": size_value,
            "isDir": bool(source.get("isDir") or source.get("is_dir") or source.get("isFolder") or source.get("fc")),
        }

    def _list_target_files(self, cid: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        limit = 200
        for _ in range(10):
            query = urllib.parse.urlencode(
                {"aid": "1", "cid": cid, "offset": offset, "limit": limit, "show_dir": "1", "o": "file_name", "asc": "1"}
            )
            payload = self._request(f"https://webapi.115.com/files?{query}")
            self._ensure_success(payload, "115 目标目录读取失败。")
            data = self._response_data(payload)
            page = data.get("data") if isinstance(data.get("data"), list) else data.get("list")
            if not isinstance(page, list):
                page = payload.get("data") if isinstance(payload.get("data"), list) else []
            rows.extend(row for row in page if isinstance(row, dict))
            if len(page) < limit:
                break
            offset += len(page)
        return rows

    def _target_has_files(self, cid: str, source_files: list[dict[str, Any]]) -> bool:
        expected = [self._file_meta(row) for row in source_files]
        expected = [row for row in expected if row["name"]]
        if not expected:
            return False
        actual = [self._file_meta(row) for row in self._list_target_files(cid)]
        for source in expected:
            source_name = self._normalize_file_name(source["name"])
            matched = False
            for target in actual:
                if self._normalize_file_name(target["name"]) != source_name:
                    continue
                if bool(source["isDir"]) != bool(target["isDir"]):
                    continue
                if not source["isDir"] and source["size"] > 0 and target["size"] != source["size"]:
                    continue
                matched = True
                break
            if not matched:
                return False
        return True

    def _submit_transfer(
        self,
        *,
        share_code: str,
        receive_code: str,
        cid: str,
        file_ids: list[str],
    ) -> dict[str, Any]:
        payload_data: dict[str, Any] = {
            "user_id": self._get_user_id(),
            "share_code": share_code,
            "receive_code": receive_code,
            "cid": cid,
        }
        if file_ids:
            payload_data["file_id"] = ",".join(file_ids)
        payload = self._request("https://webapi.115.com/share/receive", method="POST", data=payload_data)
        self._ensure_success(payload, "115 转存失败，请检查 Cookie、目录 ID、访问码或分享状态。")
        return payload

    def test_cookie(self) -> dict[str, Any]:
        payload = self._request("https://webapi.115.com/user/info")
        self._ensure_success(payload, "115 Cookie 无效或已过期。")
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        user_name = str(data.get("user_name") or data.get("nickname") or data.get("user_id") or "").strip()
        return {
            "ok": True,
            "message": f"115 Cookie 有效{f'，账号：{user_name}' if user_name else ''}。",
            "userName": user_name,
        }

    def create_qrcode_session(self, *, client: str = "qandroid") -> dict[str, Any]:
        safe_client = str(client or "qandroid").strip()
        client_meta = DRIVE115_QRCODE_CLIENTS.get(safe_client) or DRIVE115_QRCODE_CLIENTS["qandroid"]
        payload = self._request_public("https://qrcodeapi.115.com/api/1.0/web/1.0/token/")
        self._ensure_success(payload, "115 二维码创建失败。")
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        uid = str(data.get("uid") or "").strip()
        time_value = str(data.get("time") or "").strip()
        sign = str(data.get("sign") or "").strip()
        qrcode_content = str(data.get("qrcode") or data.get("qrCode") or "").strip()
        if not uid or not time_value or not sign:
            raise RuntimeError("115 二维码接口未返回完整会话参数。")
        if not qrcode_content:
            raise RuntimeError("115 二维码接口没有返回可编码的扫码内容。")
        try:
            import qrcode

            qr = qrcode.QRCode(version=None, error_correction=qrcode.constants.ERROR_CORRECT_M, box_size=8, border=3)
            qr.add_data(qrcode_content)
            qr.make(fit=True)
            image = qr.make_image(fill_color="black", back_color="white")
            buffer = BytesIO()
            image.save(buffer, format="PNG")
            image_url = f"data:image/png;base64,{base64.b64encode(buffer.getvalue()).decode('ascii')}"
        except ImportError as err:
            raise RuntimeError("服务器缺少二维码生成组件，请安装 requirements.txt 后重启。") from err
        except Exception as err:
            raise RuntimeError(f"115 二维码图片生成失败：{err}") from err
        session_id = secrets.token_urlsafe(16)
        return {
            "ok": True,
            "sessionId": session_id,
            "uid": uid,
            "time": time_value,
            "sign": sign,
            "client": safe_client if safe_client in DRIVE115_QRCODE_CLIENTS else "qandroid",
            "clientLabel": client_meta["label"],
            "imageUrl": image_url,
            "createdAt": int(time.time()),
            "expiresIn": 180,
        }

    def check_qrcode_status(self, *, uid: str, time_value: str, sign: str) -> dict[str, Any]:
        query = urllib.parse.urlencode({"uid": uid, "time": time_value, "sign": sign})
        payload = self._request_public(f"https://qrcodeapi.115.com/get/status/?{query}")
        self._ensure_success(payload, "115 二维码状态查询失败。")
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        status_raw = data.get("status") if isinstance(data, dict) else payload.get("status")
        status_text = str(status_raw if status_raw is not None else "").strip().lower()
        message = str((data.get("msg") if isinstance(data, dict) else "") or payload.get("msg") or payload.get("message") or "").strip()
        if status_text in {"0", "waiting", "wait"}:
            return {"ok": True, "status": "waiting", "message": message or "等待扫码。"}
        if status_text in {"1", "scanned", "scan"}:
            return {"ok": True, "status": "scanned", "message": message or "已扫码，请在 115 App 中确认登录。"}
        if status_text in {"2", "confirmed", "signed", "login", "success"}:
            return {"ok": True, "status": "confirmed", "message": message or "扫码已确认，正在换取 Cookie。"}
        if status_text in {"-1", "expired", "timeout"}:
            return {"ok": False, "status": "expired", "message": message or "二维码已过期，请重新生成。"}
        return {"ok": True, "status": "waiting", "message": message or f"等待扫码（状态 {status_text or '未知'}）。"}

    def login_qrcode(self, *, uid: str, client: str = "qandroid") -> dict[str, Any]:
        safe_client = str(client or "qandroid").strip()
        client_meta = DRIVE115_QRCODE_CLIENTS.get(safe_client) or DRIVE115_QRCODE_CLIENTS["qandroid"]
        app = client_meta["app"]
        url = f"https://passportapi.115.com/app/1.0/{urllib.parse.quote(app, safe='')}/1.0/login/qrcode/"
        payload, headers = self._request_public(
            url,
            method="POST",
            data={"account": str(uid or "").strip()},
            return_headers=True,
        )
        self._ensure_success(payload, "115 二维码登录失败。")
        cookies = []
        if hasattr(headers, "get_all"):
            cookies = headers.get_all("Set-Cookie") or []
        if not cookies:
            one = headers.get("Set-Cookie") if hasattr(headers, "get") else ""
            if one:
                cookies = [one]
        cookie_parts: list[str] = []
        for cookie in cookies:
            first = str(cookie or "").split(";", 1)[0].strip()
            if first and "=" in first:
                cookie_parts.append(first)
        deduped: dict[str, str] = {}
        for part in cookie_parts:
            key, value = part.split("=", 1)
            deduped[key] = value
        cookie_text = "; ".join(f"{key}={value}" for key, value in deduped.items())
        if not cookie_text:
            raise RuntimeError("115 登录成功但没有返回 Cookie，请换一个客户端类型重试。")
        return {
            "ok": True,
            "cookie": cookie_text,
            "cookieMasked": mask_cookie(cookie_text),
            "message": "115 扫码登录成功，Cookie 已获取。",
        }

    def parse_share(self, *, share_url: str, receive_code: str = "") -> dict[str, Any]:
        share = extract_115_share(share_url, receive_code)
        share_code = share.get("shareCode") or ""
        receive = share.get("receiveCode") or ""
        if not share_code:
            raise RuntimeError("未识别到 115 分享码，请粘贴完整分享链接。")
        query = urllib.parse.urlencode(
            {
                "share_code": share_code,
                "receive_code": receive,
                "cid": "0",
                "offset": "0",
                "limit": "100",
            }
        )
        payload = self._request(f"https://webapi.115.com/share/snap?{query}")
        self._ensure_success(payload, "115 分享解析失败，请检查 Cookie、分享链接和访问码。")
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        rows = data.get("list") if isinstance(data.get("list"), list) else []
        files: list[dict[str, Any]] = []
        total_size = 0
        for row in rows[:100]:
            if not isinstance(row, dict):
                continue
            file_id = str(row.get("fid") or row.get("file_id") or row.get("cid") or row.get("id") or "").strip()
            name = str(row.get("n") or row.get("file_name") or row.get("name") or "未命名资源").strip()
            size = row.get("s") or row.get("size") or 0
            try:
                total_size += int(float(size or 0))
            except Exception:
                pass
            files.append(
                {
                    "id": file_id,
                    "name": name,
                    "size": int(float(size or 0)) if str(size or "").strip() else 0,
                    "sizeText": _human_size(size),
                    "isDir": bool(row.get("is_dir") or row.get("isFolder") or row.get("fc")),
                }
            )
        title = str(data.get("share_title") or data.get("title") or (files[0]["name"] if files else "115 分享资源")).strip()
        return {
            "ok": True,
            "shareCode": share_code,
            "receiveCode": receive,
            "title": title,
            "fileCount": int(data.get("count") or len(files) or 0),
            "files": files,
            "totalSize": total_size,
            "totalSizeText": _human_size(total_size),
            "parsedAt": int(time.time()),
        }

    def transfer_share(
        self,
        *,
        share_code: str,
        receive_code: str = "",
        target_cid: str = "",
        file_ids: list[str] | None = None,
        source_files: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        safe_share_code = str(share_code or "").strip()
        if not safe_share_code:
            raise RuntimeError("缺少 115 分享码，无法转存。")
        cid = str(target_cid or self.config.get("defaultCid") or "0").strip() or "0"
        ids = [str(item).strip() for item in (file_ids or []) if str(item).strip()]
        receive = str(receive_code or "").strip()
        files = [dict(row) for row in (source_files or []) if isinstance(row, dict)]
        time.sleep(1.2)
        try:
            payload = self._submit_transfer(
                share_code=safe_share_code,
                receive_code=receive,
                cid=cid,
                file_ids=ids,
            )
        except Drive115Error as err:
            if not err.is_duplicate:
                raise
            if self._target_has_files(cid, files):
                return {
                    "ok": True,
                    "status": "exists",
                    "message": "目标目录中已存在相同文件。",
                    "targetCid": cid,
                    "shareCode": safe_share_code,
                    "errorCode": err.code,
                }
            refreshed = self.parse_share(
                share_url=f"https://115.com/s/{safe_share_code}",
                receive_code=receive,
            )
            refreshed_files = [row for row in refreshed.get("files", []) if isinstance(row, dict)]
            refreshed_ids = [str(row.get("id") or "").strip() for row in refreshed_files if str(row.get("id") or "").strip()]
            time.sleep(1.2)
            try:
                payload = self._submit_transfer(
                    share_code=safe_share_code,
                    receive_code=receive,
                    cid=cid,
                    file_ids=refreshed_ids or ids,
                )
            except Drive115Error as retry_err:
                if retry_err.is_duplicate and self._target_has_files(cid, refreshed_files or files):
                    return {
                        "ok": True,
                        "status": "exists",
                        "message": "目标目录中已存在相同文件。",
                        "targetCid": cid,
                        "shareCode": safe_share_code,
                        "errorCode": retry_err.code,
                    }
                if retry_err.is_duplicate:
                    raise Drive115Error(
                        "115 返回重复转存，但目标目录未发现相同文件，请稍后重试。",
                        code=retry_err.code,
                        payload=retry_err.payload,
                    ) from retry_err
                raise
        return {
            "ok": True,
            "status": "submitted",
            "message": "115 已收到转存请求。",
            "targetCid": cid,
            "shareCode": safe_share_code,
            "raw": payload,
        }
