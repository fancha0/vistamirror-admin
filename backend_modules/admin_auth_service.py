from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import base64
import hashlib
import hmac
import json
from pathlib import Path
import secrets
import threading
from typing import Any


def _iso_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _parse_iso(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64decode(data: str) -> bytes:
    text = str(data or "").strip()
    if not text:
        return b""
    pad = "=" * ((4 - (len(text) % 4)) % 4)
    return base64.urlsafe_b64decode(text + pad)


@dataclass
class AuthConfig:
    enabled: bool
    username: str
    password_hash: str
    session_ttl_seconds: int
    login_max_fails: int
    login_lock_seconds: int


class AdminAuthService:
    COOKIE_NAME = "vm_admin_session"
    REMEMBER_ME_COOKIE_MAX_AGE = 315360000  # 10 years

    def __init__(self, *, session_file: Path, credential_file: Path, config: AuthConfig) -> None:
        self._session_file = Path(session_file)
        self._credential_file = Path(credential_file)
        self._config = config
        self._lock = threading.Lock()
        self._sessions: dict[str, dict[str, Any]] = {}
        self._failures: dict[str, dict[str, Any]] = {}
        self._file_credentials: dict[str, Any] = {}
        self._load_credentials()
        self._load_sessions()

    @property
    def enabled(self) -> bool:
        return bool(self._config.enabled)

    def validate_startup(self) -> None:
        if not self.enabled:
            return
        env_username = str(self._config.username or "").strip()
        env_password_hash = str(self._config.password_hash or "").strip()
        if (env_username and not env_password_hash) or (env_password_hash and not env_username):
            raise RuntimeError("APP_ADMIN_USERNAME 与 APP_ADMIN_PASSWORD_HASH 需要同时设置，或同时留空。")
        if env_password_hash and not self._is_supported_password_hash(env_password_hash):
            raise RuntimeError("APP_ADMIN_PASSWORD_HASH 格式不正确，应为 pbkdf2_sha256$<iter>$<salt>$<hash>。")
        username, password_hash, source = self._resolve_credentials_unlocked()
        if not username or not password_hash:
            raise RuntimeError(
                "APP_ADMIN_AUTH_ENABLED=1 时必须提供管理员凭据：设置 APP_ADMIN_USERNAME/APP_ADMIN_PASSWORD_HASH，"
                "或在 data/admin_auth.json 中提供 username + passwordHash。"
            )
        if source == "file" and not self._is_supported_password_hash(password_hash):
            raise RuntimeError("data/admin_auth.json 中的 passwordHash 格式不正确。")

    def auth_me(self, *, cookie_header: str, client_ip: str) -> tuple[bool, str]:
        if not self.enabled:
            return True, self._config.username or "admin"
        sid = self._extract_sid(cookie_header)
        if not sid:
            return False, ""
        with self._lock:
            self._cleanup_expired_sessions_unlocked()
            row = self._sessions.get(sid)
            if not row:
                return False, ""
            if self._is_session_expired(row):
                self._sessions.pop(sid, None)
                self._save_sessions_unlocked()
                return False, ""
            if client_ip and not str(row.get("ip") or "").strip():
                row["ip"] = client_ip
                self._save_sessions_unlocked()
            current_username, _, _ = self._resolve_credentials_unlocked()
            return True, str(row.get("username") or current_username or "admin")

    def login(
        self,
        *,
        username: str,
        password: str,
        client_ip: str,
        user_agent: str,
        remember_me: bool = False,
    ) -> dict[str, Any]:
        username_input = str(username or "").strip()
        password_input = str(password or "")
        current_username, current_password_hash, _ = self._resolve_credentials_unlocked()
        if not self.enabled:
            sid = self._create_session_unlocked(
                username=current_username or username_input or "admin",
                client_ip=client_ip,
                user_agent=user_agent,
                remember_me=remember_me,
            )
            return {
                "ok": True,
                "sid": sid,
                "username": current_username or username_input or "admin",
                "rememberMe": bool(remember_me),
            }

        auth_username = current_username
        auth_password_hash = current_password_hash
        if not auth_username or not auth_password_hash:
            return {"ok": False, "status": 500, "error": "管理员凭据未配置。", "locked": False}
        key = self._failure_key(client_ip=client_ip, username=username_input)
        now = datetime.now()

        with self._lock:
            self._cleanup_failures_unlocked(now)
            failure = self._failures.get(key) or {}
            lock_until = _parse_iso(str(failure.get("lockUntil") or ""))
            if lock_until and lock_until > now:
                retry_after = int((lock_until - now).total_seconds())
                return {
                    "ok": False,
                    "status": 429,
                    "error": "登录失败次数过多，请稍后再试。",
                    "retryAfter": max(1, retry_after),
                    "locked": True,
                }

            username_ok = hmac.compare_digest(username_input, auth_username)
            password_ok = self._verify_password(password_input, auth_password_hash)
            if username_ok and password_ok:
                self._failures.pop(key, None)
                sid = self._create_session_unlocked(
                    username=auth_username,
                    client_ip=client_ip,
                    user_agent=user_agent,
                    remember_me=remember_me,
                )
                return {"ok": True, "sid": sid, "username": auth_username, "rememberMe": bool(remember_me)}

            fail_count = int(failure.get("count") or 0) + 1
            next_row: dict[str, Any] = {"count": fail_count, "updatedAt": _iso_now()}
            if fail_count >= self._config.login_max_fails:
                until = now + timedelta(seconds=self._config.login_lock_seconds)
                next_row["lockUntil"] = until.isoformat(timespec="seconds")
                self._failures[key] = next_row
                return {
                    "ok": False,
                    "status": 429,
                    "error": "登录失败次数过多，请稍后再试。",
                    "retryAfter": self._config.login_lock_seconds,
                    "locked": True,
                }
            self._failures[key] = next_row
            return {
                "ok": False,
                "status": 401,
                "error": "账号或密码错误。",
                "remaining": max(0, self._config.login_max_fails - fail_count),
                "locked": False,
            }

    def get_admin_credential_meta(self) -> dict[str, Any]:
        username, _, source = self._resolve_credentials_unlocked()
        managed_by_env = source == "env"
        return {
            "authEnabled": bool(self.enabled),
            "username": username or "",
            "managedByEnv": managed_by_env,
            "allowUpdate": bool(self.enabled and not managed_by_env),
            "source": source,
        }

    def update_admin_credentials(
        self,
        *,
        current_password: str,
        next_username: str,
        next_password: str,
    ) -> dict[str, Any]:
        if not self.enabled:
            return {"ok": False, "status": 400, "error": "当前未启用后台登录鉴权。"}
        with self._lock:
            username, stored_hash, source = self._resolve_credentials_unlocked()
            if source == "env":
                return {
                    "ok": False,
                    "status": 409,
                    "error": "当前管理员凭据由环境变量接管，请在 .env / .env.local 中修改后重启服务。",
                    "managedByEnv": True,
                }
            if not username or not stored_hash:
                return {"ok": False, "status": 500, "error": "管理员凭据不存在，请先通过环境变量初始化。"}
            if not self._verify_password(str(current_password or ""), stored_hash):
                return {"ok": False, "status": 401, "error": "当前密码校验失败。"}

            next_username_text = str(next_username or "").strip()
            next_password_text = str(next_password or "")
            if len(next_username_text) < 2:
                return {"ok": False, "status": 400, "error": "新用户名至少 2 位。"}
            if len(next_password_text) < 6:
                return {"ok": False, "status": 400, "error": "新密码至少 6 位。"}

            next_hash = self.make_password_hash(next_password_text)
            self._file_credentials = {
                "username": next_username_text,
                "passwordHash": next_hash,
                "updatedAt": _iso_now(),
            }
            self._save_credentials_unlocked()
            self._sessions.clear()
            self._save_sessions_unlocked()
            self._failures.clear()
            return {
                "ok": True,
                "status": 200,
                "username": next_username_text,
                "forcedLogout": True,
                "managedByEnv": False,
            }

    def logout(self, *, cookie_header: str) -> bool:
        sid = self._extract_sid(cookie_header)
        if not sid:
            return False
        with self._lock:
            existed = sid in self._sessions
            self._sessions.pop(sid, None)
            if existed:
                self._save_sessions_unlocked()
            return existed

    def build_set_cookie(self, *, sid: str, secure: bool, remember_me: bool = False) -> str:
        max_age = (
            self.REMEMBER_ME_COOKIE_MAX_AGE
            if remember_me
            else max(60, int(self._config.session_ttl_seconds))
        )
        parts = [
            f"{self.COOKIE_NAME}={sid}",
            "Path=/",
            "HttpOnly",
            "SameSite=Lax",
            f"Max-Age={max_age}",
        ]
        if secure:
            parts.append("Secure")
        return "; ".join(parts)

    def build_clear_cookie(self, *, secure: bool) -> str:
        parts = [
            f"{self.COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0",
        ]
        if secure:
            parts[0] = parts[0] + "; Secure"
        return parts[0]

    @staticmethod
    def make_password_hash(password: str, *, iterations: int = 260000) -> str:
        salt = secrets.token_bytes(16)
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            str(password or "").encode("utf-8"),
            salt,
            int(iterations),
        )
        return f"pbkdf2_sha256${int(iterations)}${_b64encode(salt)}${_b64encode(digest)}"

    def _extract_sid(self, cookie_header: str) -> str:
        raw = str(cookie_header or "")
        if not raw:
            return ""
        for part in raw.split(";"):
            token = part.strip()
            if not token or "=" not in token:
                continue
            key, value = token.split("=", 1)
            if key.strip() == self.COOKIE_NAME:
                return value.strip()
        return ""

    def _create_session_unlocked(
        self,
        *,
        username: str,
        client_ip: str,
        user_agent: str,
        remember_me: bool = False,
    ) -> str:
        sid = secrets.token_urlsafe(36)
        created_at = datetime.now()
        row = {
            "username": str(username or "admin"),
            "ip": str(client_ip or ""),
            "ua": str(user_agent or "")[:400],
            "createdAt": created_at.isoformat(timespec="seconds"),
            "rememberMe": bool(remember_me),
        }
        if not remember_me:
            expires_at = created_at + timedelta(seconds=self._config.session_ttl_seconds)
            row["expiresAt"] = expires_at.isoformat(timespec="seconds")
        self._sessions[sid] = row
        self._save_sessions_unlocked()
        return sid

    def _failure_key(self, *, client_ip: str, username: str) -> str:
        return f"{str(client_ip or '').strip()}::{str(username or '').strip().lower()}"

    def _resolve_credentials_unlocked(self) -> tuple[str, str, str]:
        env_username = str(self._config.username or "").strip()
        env_password_hash = str(self._config.password_hash or "").strip()
        if env_username and env_password_hash and self._is_supported_password_hash(env_password_hash):
            return env_username, env_password_hash, "env"
        file_username = str(self._file_credentials.get("username") or "").strip()
        file_password_hash = str(self._file_credentials.get("passwordHash") or "").strip()
        if file_username and file_password_hash and self._is_supported_password_hash(file_password_hash):
            return file_username, file_password_hash, "file"
        return "", "", "missing"

    @staticmethod
    def _is_supported_password_hash(raw_hash: str) -> bool:
        parts = str(raw_hash or "").split("$")
        if len(parts) != 4:
            return False
        algo, iter_text, salt_b64, digest_b64 = parts
        if algo != "pbkdf2_sha256":
            return False
        try:
            iterations = int(iter_text)
        except Exception:
            return False
        if iterations < 10000:
            return False
        try:
            _b64decode(salt_b64)
            _b64decode(digest_b64)
        except Exception:
            return False
        return True

    def _verify_password(self, password: str, stored_hash: str) -> bool:
        parts = str(stored_hash or "").split("$")
        if len(parts) != 4:
            return False
        algo, iter_text, salt_b64, digest_b64 = parts
        if algo != "pbkdf2_sha256":
            return False
        try:
            iterations = int(iter_text)
            salt = _b64decode(salt_b64)
            expected = _b64decode(digest_b64)
        except Exception:
            return False
        if not salt or not expected:
            return False
        computed = hashlib.pbkdf2_hmac("sha256", str(password or "").encode("utf-8"), salt, iterations)
        return hmac.compare_digest(computed, expected)

    def _cleanup_expired_sessions_unlocked(self) -> None:
        expired = []
        for sid, row in self._sessions.items():
            if self._is_session_expired(row):
                expired.append(sid)
        if not expired:
            return
        for sid in expired:
            self._sessions.pop(sid, None)
        self._save_sessions_unlocked()

    def _cleanup_failures_unlocked(self, now: datetime | None = None) -> None:
        now = now or datetime.now()
        stale = []
        for key, row in self._failures.items():
            lock_until = _parse_iso(str(row.get("lockUntil") or ""))
            if lock_until and lock_until > now:
                continue
            updated_at = _parse_iso(str(row.get("updatedAt") or ""))
            if updated_at and (now - updated_at).total_seconds() > max(3600, self._config.login_lock_seconds * 4):
                stale.append(key)
            elif lock_until and lock_until <= now:
                stale.append(key)
        for key in stale:
            self._failures.pop(key, None)

    def _load_sessions(self) -> None:
        self._session_file.parent.mkdir(parents=True, exist_ok=True)
        if not self._session_file.exists():
            return
        try:
            payload = json.loads(self._session_file.read_text(encoding="utf-8"))
        except Exception:
            return
        sessions = payload.get("sessions") if isinstance(payload, dict) else None
        if not isinstance(sessions, dict):
            return
        with self._lock:
            for sid, row in sessions.items():
                if not isinstance(row, dict):
                    continue
                if self._is_session_expired(row):
                    continue
                self._sessions[str(sid)] = row

    def _load_credentials(self) -> None:
        self._credential_file.parent.mkdir(parents=True, exist_ok=True)
        if not self._credential_file.exists():
            self._file_credentials = {}
            return
        try:
            payload = json.loads(self._credential_file.read_text(encoding="utf-8"))
        except Exception:
            self._file_credentials = {}
            return
        if not isinstance(payload, dict):
            self._file_credentials = {}
            return
        username = str(payload.get("username") or "").strip()
        password_hash = str(payload.get("passwordHash") or "").strip()
        if username and password_hash and self._is_supported_password_hash(password_hash):
            self._file_credentials = {
                "username": username,
                "passwordHash": password_hash,
                "updatedAt": str(payload.get("updatedAt") or "").strip(),
            }
            return
        self._file_credentials = {}

    def _is_session_expired(self, row: dict[str, Any]) -> bool:
        if bool(row.get("rememberMe")):
            return False
        expires_at = _parse_iso(str(row.get("expiresAt") or ""))
        if not expires_at:
            return True
        return expires_at <= datetime.now()

    def _save_sessions_unlocked(self) -> None:
        payload = {"sessions": self._sessions}
        self._session_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def _save_credentials_unlocked(self) -> None:
        payload = {
            "username": str(self._file_credentials.get("username") or "").strip(),
            "passwordHash": str(self._file_credentials.get("passwordHash") or "").strip(),
            "updatedAt": str(self._file_credentials.get("updatedAt") or "").strip() or _iso_now(),
        }
        self._credential_file.parent.mkdir(parents=True, exist_ok=True)
        self._credential_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
