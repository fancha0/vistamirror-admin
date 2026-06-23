from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable


HDHIVE_BASE_URL = "https://hdhive.com"
HDHIVE_SCOPES = "meta query unlock write"
HDHIVE_ENV_FIELD_MAP = {
    "authMode": "APP_HDHIVE_AUTH_MODE",
    "brokerUrl": "APP_HDHIVE_BROKER_URL",
    "clientId": "APP_HDHIVE_CLIENT_ID",
    "appSecret": "APP_HDHIVE_APP_SECRET",
    "redirectUri": "APP_HDHIVE_REDIRECT_URI",
}


class HDHiveError(RuntimeError):
    def __init__(self, message: str, *, code: str = "", status: int = 0, retry_after: int = 0) -> None:
        super().__init__(message)
        self.code = str(code or "")
        self.status = int(status or 0)
        self.retry_after = int(retry_after or 0)


def default_hdhive_config() -> dict[str, Any]:
    return {
        "enabled": False,
        "authMode": "broker",
        "brokerUrl": "",
        "installationId": "",
        "installationSecret": "",
        "oauthSessionId": "",
        "oauthSessionExpiresAt": 0,
        "autoCheckin": True,
        "timezone": "Asia/Shanghai",
        "lastCheckin": {},
        "lastCheckinDate": "",
        "clientId": "",
        "appSecret": "",
        "redirectUri": "",
        "accessToken": "",
        "refreshToken": "",
        "accessExpiresAt": 0,
        "refreshExpiresAt": 0,
        "scopes": HDHIVE_SCOPES,
        "user": {},
        "updatedAt": "",
    }


def normalize_hdhive_config(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    user = source.get("user") if isinstance(source.get("user"), dict) else {}
    configured_mode = str(source.get("authMode") or "").strip().lower()
    auth_mode = "direct" if configured_mode == "direct" or (not configured_mode and bool(source.get("clientId") or source.get("appSecret") or source.get("accessToken"))) else "broker"
    return {
        "enabled": bool(source.get("enabled")),
        "authMode": auth_mode,
        "brokerUrl": str(source.get("brokerUrl") or "").strip().rstrip("/"),
        "installationId": str(source.get("installationId") or "").strip(),
        "installationSecret": str(source.get("installationSecret") or "").strip(),
        "oauthSessionId": str(source.get("oauthSessionId") or "").strip(),
        "oauthSessionExpiresAt": int(source.get("oauthSessionExpiresAt") or 0),
        "autoCheckin": bool(source.get("autoCheckin", True)),
        "timezone": str(source.get("timezone") or "Asia/Shanghai").strip() or "Asia/Shanghai",
        "lastCheckin": source.get("lastCheckin") if isinstance(source.get("lastCheckin"), dict) else {},
        "lastCheckinDate": str(source.get("lastCheckinDate") or "").strip(),
        "clientId": str(source.get("clientId") or "").strip(),
        "appSecret": str(source.get("appSecret") or "").strip(),
        "redirectUri": str(source.get("redirectUri") or "").strip(),
        "accessToken": str(source.get("accessToken") or "").strip(),
        "refreshToken": str(source.get("refreshToken") or "").strip(),
        "accessExpiresAt": int(source.get("accessExpiresAt") or 0),
        "refreshExpiresAt": int(source.get("refreshExpiresAt") or 0),
        "scopes": str(source.get("scopes") or HDHIVE_SCOPES).strip() or HDHIVE_SCOPES,
        "user": {
            "id": str(user.get("id") or ""),
            "username": str(user.get("username") or user.get("name") or "").strip(),
            "level": str(user.get("level") or "").strip(),
            "points": user.get("points"),
            "avatar": str(user.get("avatar") or user.get("avatar_url") or "").strip(),
        },
        "updatedAt": str(source.get("updatedAt") or "").strip(),
    }


def apply_hdhive_env_overrides(raw: Any) -> dict[str, Any]:
    config = normalize_hdhive_config(raw)
    for field, env_name in HDHIVE_ENV_FIELD_MAP.items():
        value = str(os.environ.get(env_name) or "").strip()
        if value:
            config[field] = value
    if not str(os.environ.get("APP_HDHIVE_AUTH_MODE") or "").strip() and not config.get("brokerUrl"):
        if str(os.environ.get("APP_HDHIVE_CLIENT_ID") or "").strip() and str(os.environ.get("APP_HDHIVE_APP_SECRET") or "").strip():
            config["authMode"] = "direct"
    return config


def env_managed_hdhive_fields() -> list[str]:
    return [field for field, env_name in HDHIVE_ENV_FIELD_MAP.items() if str(os.environ.get(env_name) or "").strip()]


def merge_hdhive_config_for_save(current: Any, incoming: Any) -> dict[str, Any]:
    existing = normalize_hdhive_config(current)
    source = incoming if isinstance(incoming, dict) else {}
    merged = dict(existing)
    for field in ("enabled", "authMode", "brokerUrl", "autoCheckin", "timezone", "clientId", "redirectUri"):
        if field in source:
            merged[field] = source.get(field)
    submitted_secret = str(source.get("appSecret") or "").strip()
    if submitted_secret and submitted_secret != mask_secret(str(existing.get("appSecret") or "")):
        merged["appSecret"] = submitted_secret
    merged["updatedAt"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return normalize_hdhive_config(merged)


def mask_secret(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= 10:
        return "******"
    return f"{text[:4]}...{text[-4:]}"


def public_hdhive_config(raw: Any) -> dict[str, Any]:
    config = normalize_hdhive_config(raw)
    return {
        "enabled": config["enabled"],
        "authMode": config["authMode"],
        "brokerUrl": config["brokerUrl"],
        "installationId": config["installationId"],
        "registered": bool(config["installationId"] and config["installationSecret"]),
        "oauthSessionId": config["oauthSessionId"],
        "oauthSessionExpiresAt": config["oauthSessionExpiresAt"],
        "autoCheckin": config["autoCheckin"],
        "timezone": config["timezone"],
        "lastCheckin": config["lastCheckin"],
        "lastCheckinDate": config["lastCheckinDate"],
        "clientId": config["clientId"],
        "redirectUri": config["redirectUri"],
        "scopes": config["scopes"],
        "updatedAt": config["updatedAt"],
        "hasAppSecret": bool(config["appSecret"]),
        "appSecretMasked": mask_secret(config["appSecret"]),
        "authorized": bool(config["user"]) if config["authMode"] == "broker" else bool(config["accessToken"] or config["refreshToken"]),
        "accessExpiresAt": config["accessExpiresAt"],
        "user": config["user"],
    }


@dataclass
class HDHiveService:
    config: dict[str, Any]
    save_config: Callable[[dict[str, Any]], None] | None = None
    timeout: int = 25

    def __post_init__(self) -> None:
        self.config = apply_hdhive_env_overrides(self.config)

    @property
    def is_broker(self) -> bool:
        return str(self.config.get("authMode") or "broker") == "broker"

    def _broker_request(
        self,
        path: str,
        *,
        method: str = "GET",
        body: dict[str, Any] | None = None,
        authenticated: bool = True,
    ) -> dict[str, Any]:
        base_url = str(self.config.get("brokerUrl") or "").strip().rstrip("/")
        if not base_url.startswith(("https://", "http://127.0.0.1", "http://localhost")):
            raise HDHiveError("请配置 HTTPS 影巢授权代理地址。", code="MISSING_BROKER_URL")
        headers = {"Accept": "application/json", "User-Agent": "Vistamirror/1.0"}
        if authenticated:
            installation_id = str(self.config.get("installationId") or "").strip()
            installation_secret = str(self.config.get("installationSecret") or "").strip()
            if not installation_id or not installation_secret:
                raise HDHiveError("影巢代理安装尚未注册。", code="BROKER_NOT_REGISTERED", status=401)
            headers["X-Installation-ID"] = installation_id
            headers["Authorization"] = f"Bearer {installation_secret}"
        data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(base_url + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                payload = self._decode(response.read())
        except urllib.error.HTTPError as err:
            try:
                payload = self._decode(err.read())
            except HDHiveError:
                payload = {}
            raise HDHiveError(
                str(payload.get("error") or payload.get("message") or f"影巢代理失败（HTTP {err.code}）"),
                code=str(payload.get("code") or ""), status=err.code,
                retry_after=int(err.headers.get("Retry-After") or 0),
            ) from err
        except urllib.error.URLError as err:
            raise HDHiveError(f"无法连接影巢授权代理：{getattr(err, 'reason', err)}") from err
        if not payload.get("ok", False):
            raise HDHiveError(str(payload.get("error") or "影巢代理请求失败。"), code=str(payload.get("code") or ""))
        return payload

    def register_installation(self) -> dict[str, Any]:
        if not self.is_broker:
            return self.config
        if self.config.get("installationId") and self.config.get("installationSecret"):
            return self.config
        payload = self._broker_request("/v1/installations/register", method="POST", body={}, authenticated=False)
        self.config["installationId"] = str(payload.get("installationId") or "")
        self.config["installationSecret"] = str(payload.get("installationSecret") or "")
        if not self.config["installationId"] or not self.config["installationSecret"]:
            raise HDHiveError("代理未返回安装凭据。")
        self._persist()
        return self.config

    def create_broker_oauth_session(self) -> dict[str, Any]:
        self.register_installation()
        payload = self._broker_request("/v1/oauth/sessions", method="POST", body={})
        self.config["oauthSessionId"] = str(payload.get("sessionId") or "")
        self.config["oauthSessionExpiresAt"] = int(payload.get("expiresAt") or 0)
        self._persist()
        return payload

    def broker_oauth_status(self, session_id: str = "") -> dict[str, Any]:
        target = str(session_id or self.config.get("oauthSessionId") or "").strip()
        if not target:
            raise HDHiveError("没有等待中的影巢授权会话。")
        payload = self._broker_request(f"/v1/oauth/sessions/{urllib.parse.quote(target, safe='')}")
        if str(payload.get("status") or "") == "authorized":
            self.config["user"] = payload.get("user") if isinstance(payload.get("user"), dict) else {}
            self.config["oauthSessionId"] = ""
            self.config["oauthSessionExpiresAt"] = 0
            self._persist()
            self.update_broker_preferences()
        return payload

    def update_broker_preferences(self) -> None:
        if not self.is_broker or not self.config.get("installationId"):
            return
        self._broker_request(
            "/v1/preferences", method="POST",
            body={"autoCheckin": bool(self.config.get("autoCheckin", True)), "timezone": str(self.config.get("timezone") or "Asia/Shanghai")},
        )

    def _headers(self, *, user: bool) -> dict[str, str]:
        secret = str(self.config.get("appSecret") or "").strip()
        if not secret:
            raise HDHiveError("影巢应用 Secret 未配置。", code="MISSING_API_KEY", status=401)
        headers = {"X-API-Key": secret, "Accept": "application/json", "User-Agent": "Vistamirror/1.0"}
        if user:
            token = str(self.config.get("accessToken") or "").strip()
            if not token:
                raise HDHiveError("影巢账号尚未授权。", code="OPENAPI_USER_REQUIRED", status=401)
            headers["Authorization"] = f"Bearer {token}"
        return headers

    @staticmethod
    def _decode(raw: bytes) -> dict[str, Any]:
        try:
            payload = json.loads(raw.decode("utf-8", errors="replace"))
        except Exception as err:
            raise HDHiveError("影巢返回内容无法解析。") from err
        if not isinstance(payload, dict):
            raise HDHiveError("影巢返回格式异常。")
        return payload

    def _request(
        self,
        path: str,
        *,
        method: str = "GET",
        body: dict[str, Any] | None = None,
        user: bool = True,
        retry_refresh: bool = True,
    ) -> dict[str, Any]:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
        headers = self._headers(user=user)
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(f"{HDHIVE_BASE_URL}{path}", data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                payload = self._decode(response.read())
        except urllib.error.HTTPError as err:
            raw = err.read()
            try:
                payload = self._decode(raw)
            except HDHiveError:
                payload = {}
            code = str(payload.get("code") or "")
            message = str(payload.get("description") or payload.get("message") or f"影巢接口失败（HTTP {err.code}）")
            retry_after = int(err.headers.get("Retry-After") or payload.get("retry_after_seconds") or 0)
            if user and retry_refresh and code == "OPENAPI_REFRESH_REQUIRED":
                self.refresh_token()
                return self._request(path, method=method, body=body, user=user, retry_refresh=False)
            raise HDHiveError(message, code=code, status=err.code, retry_after=retry_after) from err
        except urllib.error.URLError as err:
            raise HDHiveError(f"无法连接影巢：{getattr(err, 'reason', err)}") from err
        if not payload.get("success", False):
            raise HDHiveError(
                str(payload.get("description") or payload.get("message") or "影巢请求失败。"),
                code=str(payload.get("code") or ""),
            )
        return payload

    def ping(self) -> dict[str, Any]:
        if self.is_broker:
            self.register_installation()
            return self._broker_request("/healthz", authenticated=False)
        return self._request("/api/open/ping", user=False).get("data") or {}

    def build_authorize_url(self, *, state: str, redirect_uri: str) -> str:
        client_id = str(self.config.get("clientId") or "").strip()
        if not client_id or not str(self.config.get("appSecret") or "").strip():
            raise HDHiveError("请先保存影巢 Client ID 和应用 Secret。")
        params = urllib.parse.urlencode(
            {
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "scope": HDHIVE_SCOPES,
                "state": state,
                "response_mode": "redirect",
            }
        )
        return f"{HDHIVE_BASE_URL}/openapi/authorize?{params}"

    def exchange_code(self, *, code: str, redirect_uri: str) -> dict[str, Any]:
        payload = self._request(
            "/api/public/openapi/oauth/token",
            method="POST",
            body={"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri},
            user=False,
        )
        return self._apply_token_payload(payload.get("data") or {})

    def refresh_token(self) -> dict[str, Any]:
        refresh = str(self.config.get("refreshToken") or "").strip()
        if not refresh:
            raise HDHiveError("影巢授权已失效，请重新授权。", code="OPENAPI_REAUTH_REQUIRED", status=401)
        payload = self._request(
            "/api/public/openapi/oauth/refresh",
            method="POST",
            body={"refresh_token": refresh},
            user=False,
        )
        return self._apply_token_payload(payload.get("data") or {})

    def _apply_token_payload(self, data: dict[str, Any]) -> dict[str, Any]:
        now = int(time.time())
        access_token = str(data.get("access_token") or "").strip()
        if not access_token:
            raise HDHiveError("影巢授权未返回 Access Token。")
        self.config["accessToken"] = access_token
        if str(data.get("refresh_token") or "").strip():
            self.config["refreshToken"] = str(data.get("refresh_token") or "").strip()
        self.config["accessExpiresAt"] = now + max(0, int(data.get("expires_in") or 0))
        if data.get("refresh_expires_in"):
            self.config["refreshExpiresAt"] = now + max(0, int(data.get("refresh_expires_in") or 0))
        self.config["scopes"] = str(data.get("scope") or HDHIVE_SCOPES).strip() or HDHIVE_SCOPES
        self._persist()
        return self.config

    def me(self) -> dict[str, Any]:
        if self.is_broker:
            payload = self._broker_request("/v1/me")
            data = payload.get("data") or {}
            self.config["user"] = data if isinstance(data, dict) else {}
            self.config["scopes"] = str(payload.get("scopes") or HDHIVE_SCOPES)
            self.config["autoCheckin"] = bool(payload.get("autoCheckin", True))
            self.config["timezone"] = str(payload.get("timezone") or "Asia/Shanghai")
            self.config["lastCheckin"] = payload.get("lastCheckin") if isinstance(payload.get("lastCheckin"), dict) else {}
            self._persist()
            return self.config["user"]
        data = self._request("/api/open/me").get("data") or {}
        if isinstance(data, dict):
            self.config["user"] = {
                "id": str(data.get("id") or ""),
                "username": str(data.get("username") or data.get("name") or "").strip(),
                "level": str(data.get("level") or "").strip(),
                "points": data.get("points"),
                "avatar": str(data.get("avatar") or data.get("avatar_url") or "").strip(),
            }
            self._persist()
        return data if isinstance(data, dict) else {}

    def search_resources(self, *, media_type: str, tmdb_id: str) -> dict[str, Any]:
        target_type = "tv" if str(media_type).lower() in {"tv", "series"} else "movie"
        if self.is_broker:
            payload = self._broker_request("/v1/search", method="POST", body={"mediaType": target_type, "tmdbId": str(tmdb_id or "")})
            data = payload.get("data")
            rows = data if isinstance(data, list) else data.get("items") if isinstance(data, dict) else []
            return {"items": rows if isinstance(rows, list) else [], "meta": payload.get("meta") or {}}
        safe_id = urllib.parse.quote(str(tmdb_id or "").strip(), safe="")
        payload = self._request(f"/api/open/resources/{target_type}/{safe_id}")
        data = payload.get("data")
        rows = data if isinstance(data, list) else data.get("items") if isinstance(data, dict) else []
        return {"items": rows if isinstance(rows, list) else [], "meta": payload.get("meta") or {}}

    def unlock(self, slug: str) -> dict[str, Any]:
        if self.is_broker:
            data = self._broker_request("/v1/unlock", method="POST", body={"slug": str(slug or "").strip()}).get("data") or {}
            if not isinstance(data, dict) or not str(data.get("full_url") or data.get("url") or "").strip():
                raise HDHiveError("影巢解锁成功，但没有返回可用分享链接。")
            return data
        data = self._request("/api/open/resources/unlock", method="POST", body={"slug": str(slug or "").strip()}).get("data") or {}
        if not isinstance(data, dict) or not str(data.get("full_url") or data.get("url") or "").strip():
            raise HDHiveError("影巢解锁成功，但没有返回可用分享链接。")
        return data

    def checkin(self) -> dict[str, Any]:
        if self.is_broker:
            data = self._broker_request("/v1/checkin", method="POST", body={}).get("data") or {}
        else:
            data = self._request("/api/open/checkin", method="POST", body={"is_gambler": False}).get("data") or {}
        self.config["lastCheckin"] = data if isinstance(data, dict) else {}
        try:
            from zoneinfo import ZoneInfo
            self.config["lastCheckinDate"] = datetime.now(ZoneInfo(str(self.config.get("timezone") or "Asia/Shanghai"))).date().isoformat()
        except Exception:
            self.config["lastCheckinDate"] = datetime.now(timezone.utc).date().isoformat()
        self._persist()
        return self.config["lastCheckin"]

    def disconnect(self) -> None:
        if self.is_broker and self.config.get("installationId"):
            self._broker_request("/v1/disconnect", method="POST", body={})
        elif self.config.get("refreshToken"):
            try:
                self._request(
                    "/api/public/openapi/oauth/revoke", method="POST",
                    body={"refresh_token": str(self.config.get("refreshToken") or "")}, user=False,
                )
            except HDHiveError:
                pass
        for field in ("accessToken", "refreshToken", "oauthSessionId"):
            self.config[field] = ""
        for field in ("accessExpiresAt", "refreshExpiresAt", "oauthSessionExpiresAt"):
            self.config[field] = 0
        self.config["user"] = {}
        self.config["lastCheckin"] = {}
        self.config["lastCheckinDate"] = ""
        self._persist()

    def _persist(self) -> None:
        if self.save_config:
            self.save_config(normalize_hdhive_config(self.config))
