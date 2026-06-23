from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
import pathlib
import secrets
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

try:
    from Crypto.Cipher import AES
except ImportError:  # Local source runs may not have installed requirements yet.
    AES = None  # type: ignore[assignment]


HDHIVE_BASE_URL = "https://hdhive.com"
HDHIVE_SCOPES = "meta query unlock write"
DB_LOCK = threading.RLock()
RATE_LOCK = threading.RLock()
RATE_BUCKETS: dict[str, list[float]] = {}


class BrokerError(RuntimeError):
    def __init__(self, message: str, *, status: int = 400, code: str = "") -> None:
        super().__init__(message)
        self.status = status
        self.code = code


def _now() -> int:
    return int(time.time())


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class BrokerStore:
    def __init__(self, path: pathlib.Path, encryption_key: str) -> None:
        if AES is None:
            raise RuntimeError("缺少 pycryptodome，请先安装 requirements.txt")
        if len(encryption_key) < 24:
            raise RuntimeError("HDHIVE_BROKER_ENCRYPTION_KEY 至少需要 24 个字符")
        self.path = path
        self.key = hashlib.sha256(encryption_key.encode("utf-8")).digest()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=15)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with DB_LOCK, self.connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS installations (
                    id TEXT PRIMARY KEY,
                    secret_hash TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    last_seen_at INTEGER NOT NULL,
                    disabled INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS oauth_sessions (
                    id TEXT PRIMARY KEY,
                    installation_id TEXT NOT NULL,
                    state TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    error TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS grants (
                    installation_id TEXT PRIMARY KEY,
                    token_blob TEXT NOT NULL,
                    user_json TEXT NOT NULL DEFAULT '{}',
                    scopes TEXT NOT NULL DEFAULT '',
                    access_expires_at INTEGER NOT NULL DEFAULT 0,
                    refresh_expires_at INTEGER NOT NULL DEFAULT 0,
                    auto_checkin INTEGER NOT NULL DEFAULT 1,
                    timezone_name TEXT NOT NULL DEFAULT 'Asia/Shanghai',
                    last_checkin_date TEXT NOT NULL DEFAULT '',
                    last_checkin_json TEXT NOT NULL DEFAULT '{}',
                    updated_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_oauth_state ON oauth_sessions(state);
                """
            )

    @staticmethod
    def secret_hash(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def encrypt(self, value: dict[str, Any]) -> str:
        cipher = AES.new(self.key, AES.MODE_GCM)
        ciphertext, tag = cipher.encrypt_and_digest(_json_bytes(value))
        return base64.urlsafe_b64encode(cipher.nonce + tag + ciphertext).decode("ascii")

    def decrypt(self, blob: str) -> dict[str, Any]:
        raw = base64.urlsafe_b64decode(blob.encode("ascii"))
        nonce, tag, ciphertext = raw[:16], raw[16:32], raw[32:]
        cipher = AES.new(self.key, AES.MODE_GCM, nonce=nonce)
        value = json.loads(cipher.decrypt_and_verify(ciphertext, tag).decode("utf-8"))
        return value if isinstance(value, dict) else {}

    def register(self) -> dict[str, str]:
        installation_id = "ins_" + secrets.token_urlsafe(18)
        installation_secret = secrets.token_urlsafe(40)
        now = _now()
        with DB_LOCK, self.connect() as conn:
            conn.execute(
                "INSERT INTO installations(id, secret_hash, created_at, last_seen_at) VALUES(?,?,?,?)",
                (installation_id, self.secret_hash(installation_secret), now, now),
            )
        return {"installationId": installation_id, "installationSecret": installation_secret}

    def authenticate(self, installation_id: str, installation_secret: str) -> None:
        with DB_LOCK, self.connect() as conn:
            row = conn.execute("SELECT * FROM installations WHERE id=?", (installation_id,)).fetchone()
            if not row or row["disabled"] or not secrets.compare_digest(row["secret_hash"], self.secret_hash(installation_secret)):
                raise BrokerError("安装凭据无效。", status=401, code="INVALID_INSTALLATION")
            conn.execute("UPDATE installations SET last_seen_at=? WHERE id=?", (_now(), installation_id))

    def rotate_secret(self, installation_id: str) -> str:
        new_secret = secrets.token_urlsafe(40)
        with DB_LOCK, self.connect() as conn:
            conn.execute("UPDATE installations SET secret_hash=?,last_seen_at=? WHERE id=?", (self.secret_hash(new_secret), _now(), installation_id))
        return new_secret

    def create_oauth_session(self, installation_id: str) -> dict[str, Any]:
        session_id = "oas_" + secrets.token_urlsafe(20)
        state = secrets.token_urlsafe(32)
        now = _now()
        with DB_LOCK, self.connect() as conn:
            conn.execute(
                "INSERT INTO oauth_sessions(id, installation_id, state, status, created_at, expires_at) VALUES(?,?,?,?,?,?)",
                (session_id, installation_id, state, "pending", now, now + 600),
            )
        return {"sessionId": session_id, "state": state, "expiresAt": now + 600}

    def oauth_by_state(self, state: str) -> sqlite3.Row | None:
        with DB_LOCK, self.connect() as conn:
            return conn.execute("SELECT * FROM oauth_sessions WHERE state=?", (state,)).fetchone()

    def oauth_status(self, installation_id: str, session_id: str) -> dict[str, Any]:
        with DB_LOCK, self.connect() as conn:
            row = conn.execute(
                "SELECT status,error,expires_at FROM oauth_sessions WHERE id=? AND installation_id=?",
                (session_id, installation_id),
            ).fetchone()
        if not row:
            raise BrokerError("授权会话不存在。", status=404)
        status = row["status"]
        if row["expires_at"] < _now() and status == "pending":
            status = "expired"
        return {"status": status, "error": row["error"], "expiresAt": row["expires_at"]}

    def finish_oauth(self, state: str, tokens: dict[str, Any], user: dict[str, Any]) -> None:
        row = self.oauth_by_state(state)
        if not row or row["expires_at"] < _now() or row["status"] != "pending":
            raise BrokerError("授权 state 无效、已使用或已过期。", status=400)
        now = _now()
        scopes = str(tokens.get("scope") or HDHIVE_SCOPES)
        blob = self.encrypt(tokens)
        with DB_LOCK, self.connect() as conn:
            conn.execute(
                """INSERT INTO grants(installation_id,token_blob,user_json,scopes,access_expires_at,refresh_expires_at,updated_at)
                   VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(installation_id) DO UPDATE SET token_blob=excluded.token_blob,user_json=excluded.user_json,
                   scopes=excluded.scopes,access_expires_at=excluded.access_expires_at,
                   refresh_expires_at=excluded.refresh_expires_at,updated_at=excluded.updated_at""",
                (
                    row["installation_id"], blob, json.dumps(user, ensure_ascii=False), scopes,
                    now + int(tokens.get("expires_in") or 0), now + int(tokens.get("refresh_expires_in") or 0), now,
                ),
            )
            conn.execute("UPDATE oauth_sessions SET status='authorized' WHERE state=?", (state,))

    def fail_oauth(self, state: str, message: str) -> None:
        with DB_LOCK, self.connect() as conn:
            conn.execute("UPDATE oauth_sessions SET status='failed',error=? WHERE state=?", (message[:500], state))

    def grant(self, installation_id: str) -> dict[str, Any]:
        with DB_LOCK, self.connect() as conn:
            row = conn.execute("SELECT * FROM grants WHERE installation_id=?", (installation_id,)).fetchone()
        if not row:
            raise BrokerError("影巢账号尚未授权。", status=401, code="OPENAPI_USER_REQUIRED")
        value = dict(row)
        value["tokens"] = self.decrypt(row["token_blob"])
        value["user"] = json.loads(row["user_json"] or "{}")
        value["lastCheckin"] = json.loads(row["last_checkin_json"] or "{}")
        return value

    def save_grant(self, installation_id: str, tokens: dict[str, Any], *, user: dict[str, Any] | None = None) -> None:
        current = self.grant(installation_id)
        now = _now()
        with DB_LOCK, self.connect() as conn:
            conn.execute(
                """UPDATE grants SET token_blob=?,user_json=?,scopes=?,access_expires_at=?,refresh_expires_at=?,updated_at=?
                   WHERE installation_id=?""",
                (
                    self.encrypt(tokens), json.dumps(user if user is not None else current["user"], ensure_ascii=False),
                    str(tokens.get("scope") or current["scopes"]), now + int(tokens.get("expires_in") or 0),
                    now + int(tokens.get("refresh_expires_in") or 0), now, installation_id,
                ),
            )

    def set_preferences(self, installation_id: str, auto_checkin: bool, timezone_name: str) -> None:
        with DB_LOCK, self.connect() as conn:
            conn.execute(
                "UPDATE grants SET auto_checkin=?,timezone_name=?,updated_at=? WHERE installation_id=?",
                (1 if auto_checkin else 0, timezone_name or "Asia/Shanghai", _now(), installation_id),
            )

    def save_checkin(self, installation_id: str, result: dict[str, Any], date_key: str) -> None:
        with DB_LOCK, self.connect() as conn:
            conn.execute(
                "UPDATE grants SET last_checkin_date=?,last_checkin_json=?,updated_at=? WHERE installation_id=?",
                (date_key, json.dumps(result, ensure_ascii=False), _now(), installation_id),
            )

    def disconnect(self, installation_id: str) -> None:
        with DB_LOCK, self.connect() as conn:
            conn.execute("DELETE FROM grants WHERE installation_id=?", (installation_id,))
            conn.execute("DELETE FROM oauth_sessions WHERE installation_id=?", (installation_id,))

    def auto_checkin_installations(self) -> list[str]:
        with DB_LOCK, self.connect() as conn:
            rows = conn.execute("SELECT installation_id FROM grants WHERE auto_checkin=1").fetchall()
        return [str(row["installation_id"]) for row in rows]


class HDHiveBroker:
    def __init__(self) -> None:
        self.client_id = str(os.environ.get("HDHIVE_BROKER_CLIENT_ID") or "").strip()
        self.app_secret = str(os.environ.get("HDHIVE_BROKER_APP_SECRET") or "").strip()
        self.public_url = str(os.environ.get("HDHIVE_BROKER_PUBLIC_URL") or "").strip().rstrip("/")
        data_dir = pathlib.Path(str(os.environ.get("HDHIVE_BROKER_DATA_DIR") or "/app/data")).expanduser()
        self.store = BrokerStore(data_dir / "hdhive_broker.sqlite3", str(os.environ.get("HDHIVE_BROKER_ENCRYPTION_KEY") or ""))
        if not self.client_id or not self.app_secret or not self.public_url.startswith("https://"):
            raise RuntimeError("必须配置 HDHIVE_BROKER_CLIENT_ID、HDHIVE_BROKER_APP_SECRET 和 HTTPS 公网地址")

    @property
    def callback_uri(self) -> str:
        return self.public_url + "/oauth/callback"

    def hdhive_request(self, path: str, *, method: str = "GET", body: dict[str, Any] | None = None, token: str = "") -> dict[str, Any]:
        headers = {"X-API-Key": self.app_secret, "Accept": "application/json", "User-Agent": "Vistamirror-Broker/1.0"}
        if token:
            headers["Authorization"] = "Bearer " + token
        data = _json_bytes(body) if body is not None else None
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(HDHIVE_BASE_URL + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=25) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as err:
            try:
                payload = json.loads(err.read().decode("utf-8"))
            except Exception:
                payload = {}
            raise BrokerError(
                str(payload.get("description") or payload.get("message") or f"影巢接口失败（HTTP {err.code}）"),
                status=err.code, code=str(payload.get("code") or ""),
            ) from err
        except urllib.error.URLError as err:
            raise BrokerError(f"无法连接影巢：{getattr(err, 'reason', err)}", status=502) from err
        if not isinstance(payload, dict) or not payload.get("success", False):
            raise BrokerError(str(payload.get("description") or payload.get("message") or "影巢请求失败。"), code=str(payload.get("code") or ""))
        return payload

    def authorize_url(self, state: str) -> str:
        return HDHIVE_BASE_URL + "/openapi/authorize?" + urllib.parse.urlencode(
            {"client_id": self.client_id, "redirect_uri": self.callback_uri, "scope": HDHIVE_SCOPES, "state": state, "response_mode": "redirect"}
        )

    def exchange(self, code: str) -> dict[str, Any]:
        return self.hdhive_request(
            "/api/public/openapi/oauth/token", method="POST",
            body={"grant_type": "authorization_code", "code": code, "redirect_uri": self.callback_uri},
        ).get("data") or {}

    def refresh(self, installation_id: str, grant: dict[str, Any]) -> dict[str, Any]:
        refresh_token = str(grant["tokens"].get("refresh_token") or "")
        if not refresh_token:
            raise BrokerError("授权已失效，请重新授权。", status=401, code="OPENAPI_REAUTH_REQUIRED")
        tokens = self.hdhive_request("/api/public/openapi/oauth/refresh", method="POST", body={"refresh_token": refresh_token}).get("data") or {}
        if not tokens.get("refresh_token"):
            tokens["refresh_token"] = refresh_token
        self.store.save_grant(installation_id, tokens)
        return self.store.grant(installation_id)

    def user_request(self, installation_id: str, path: str, *, method: str = "GET", body: dict[str, Any] | None = None) -> dict[str, Any]:
        grant = self.store.grant(installation_id)
        try:
            return self.hdhive_request(path, method=method, body=body, token=str(grant["tokens"].get("access_token") or ""))
        except BrokerError as err:
            if err.code != "OPENAPI_REFRESH_REQUIRED":
                raise
        grant = self.refresh(installation_id, grant)
        return self.hdhive_request(path, method=method, body=body, token=str(grant["tokens"].get("access_token") or ""))

    def me(self, installation_id: str) -> dict[str, Any]:
        user = self.user_request(installation_id, "/api/open/me").get("data") or {}
        grant = self.store.grant(installation_id)
        self.store.save_grant(installation_id, grant["tokens"], user=user if isinstance(user, dict) else {})
        return user if isinstance(user, dict) else {}

    def checkin(self, installation_id: str) -> dict[str, Any]:
        result = self.user_request(installation_id, "/api/open/checkin", method="POST", body={"is_gambler": False}).get("data") or {}
        grant = self.store.grant(installation_id)
        try:
            from zoneinfo import ZoneInfo
            date_key = datetime.now(ZoneInfo(str(grant["timezone_name"] or "Asia/Shanghai"))).date().isoformat()
        except Exception:
            date_key = datetime.now(timezone.utc).date().isoformat()
        self.store.save_checkin(installation_id, result if isinstance(result, dict) else {}, date_key)
        return result if isinstance(result, dict) else {}

    def disconnect(self, installation_id: str) -> None:
        try:
            grant = self.store.grant(installation_id)
            refresh_token = str(grant["tokens"].get("refresh_token") or "")
            if refresh_token:
                self.hdhive_request("/api/public/openapi/oauth/revoke", method="POST", body={"refresh_token": refresh_token})
        except BrokerError:
            pass
        finally:
            self.store.disconnect(installation_id)


BROKER: HDHiveBroker | None = None


class BrokerHandler(BaseHTTPRequestHandler):
    server_version = "VistamirrorHDHiveBroker/1.0"

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        raw = _json_bytes(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        if status == 429:
            self.send_header("Retry-After", "60")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _body(self) -> dict[str, Any]:
        size = int(self.headers.get("Content-Length") or 0)
        if size > 1024 * 1024:
            raise BrokerError("请求体过大。", status=413)
        if not size:
            return {}
        value = json.loads(self.rfile.read(size).decode("utf-8"))
        return value if isinstance(value, dict) else {}

    def _auth(self) -> str:
        installation_id = str(self.headers.get("X-Installation-ID") or "").strip()
        secret_value = str(self.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
        if not installation_id or not secret_value:
            raise BrokerError("缺少安装凭据。", status=401)
        assert BROKER is not None
        BROKER.store.authenticate(installation_id, secret_value)
        self._rate_limit(installation_id)
        return installation_id

    @staticmethod
    def _rate_limit(key: str) -> None:
        now = time.time()
        limit = max(10, int(os.environ.get("HDHIVE_BROKER_RATE_LIMIT_PER_MINUTE") or 120))
        with RATE_LOCK:
            bucket = [stamp for stamp in RATE_BUCKETS.get(key, []) if now - stamp < 60]
            if len(bucket) >= limit:
                raise BrokerError("请求过于频繁，请稍后重试。", status=429, code="RATE_LIMITED")
            bucket.append(now)
            RATE_BUCKETS[key] = bucket

    def do_POST(self) -> None:
        assert BROKER is not None
        try:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            if path == "/v1/installations/register":
                self._rate_limit("register:" + str(self.client_address[0]))
                self._send_json(201, {"ok": True, **BROKER.store.register()})
                return
            installation_id = self._auth()
            body = self._body()
            if path == "/v1/installations/rotate":
                self._send_json(200, {"ok": True, "installationSecret": BROKER.store.rotate_secret(installation_id)})
                return
            if path == "/v1/oauth/sessions":
                session = BROKER.store.create_oauth_session(installation_id)
                session["authorizeUrl"] = BROKER.authorize_url(session.pop("state"))
                self._send_json(201, {"ok": True, **session})
                return
            if path == "/v1/preferences":
                BROKER.store.set_preferences(installation_id, bool(body.get("autoCheckin", True)), str(body.get("timezone") or "Asia/Shanghai"))
                self._send_json(200, {"ok": True})
                return
            if path == "/v1/search":
                media_type = "tv" if str(body.get("mediaType") or "").lower() in {"tv", "series"} else "movie"
                tmdb_id = urllib.parse.quote(str(body.get("tmdbId") or ""), safe="")
                data = BROKER.user_request(installation_id, f"/api/open/resources/{media_type}/{tmdb_id}")
                self._send_json(200, {"ok": True, "data": data.get("data"), "meta": data.get("meta") or {}})
                return
            if path == "/v1/unlock":
                data = BROKER.user_request(installation_id, "/api/open/resources/unlock", method="POST", body={"slug": str(body.get("slug") or "")})
                self._send_json(200, {"ok": True, "data": data.get("data") or {}})
                return
            if path == "/v1/checkin":
                self._send_json(200, {"ok": True, "data": BROKER.checkin(installation_id)})
                return
            if path == "/v1/disconnect":
                BROKER.disconnect(installation_id)
                self._send_json(200, {"ok": True})
                return
            raise BrokerError("接口不存在。", status=404)
        except BrokerError as err:
            self._send_json(err.status, {"ok": False, "code": err.code, "error": str(err)})
        except Exception as err:
            print(f"[hdhive-broker] POST {urllib.parse.urlparse(self.path).path} failed: {type(err).__name__}")
            self._send_json(500, {"ok": False, "error": "代理服务内部错误。"})

    def do_GET(self) -> None:
        assert BROKER is not None
        try:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            params = urllib.parse.parse_qs(parsed.query)
            if path == "/healthz":
                self._send_json(200, {"ok": True, "service": "hdhive-broker"})
                return
            if path == "/oauth/callback":
                state = str((params.get("state") or [""])[0])
                code = str((params.get("code") or [""])[0])
                error = str((params.get("error_description") or params.get("error") or [""])[0])
                if error or not state or not code:
                    if state:
                        BROKER.store.fail_oauth(state, error or "影巢未返回授权码")
                    raise BrokerError(error or "授权参数不完整。")
                tokens = BROKER.exchange(code)
                access = str(tokens.get("access_token") or "")
                user = BROKER.hdhive_request("/api/open/me", token=access).get("data") or {}
                BROKER.store.finish_oauth(state, tokens, user if isinstance(user, dict) else {})
                content = "<!doctype html><meta charset='utf-8'><title>授权成功</title><style>body{font-family:sans-serif;display:grid;place-items:center;height:100vh;margin:0;background:#f4faf7;color:#173d32}.box{padding:36px;border:1px solid #cfe5dc;border-radius:8px;background:white;text-align:center}</style><div class='box'><h1>影巢授权成功</h1><p>可以关闭此页面并返回 Vistamirror。</p></div>".encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return
            installation_id = self._auth()
            if path.startswith("/v1/oauth/sessions/"):
                session_id = path.rsplit("/", 1)[-1]
                status = BROKER.store.oauth_status(installation_id, session_id)
                if status["status"] == "authorized":
                    status["user"] = BROKER.me(installation_id)
                self._send_json(200, {"ok": True, **status})
                return
            if path == "/v1/me":
                grant = BROKER.store.grant(installation_id)
                user = BROKER.me(installation_id)
                self._send_json(200, {"ok": True, "data": user, "scopes": grant["scopes"], "autoCheckin": bool(grant["auto_checkin"]), "timezone": grant["timezone_name"], "lastCheckin": grant["lastCheckin"]})
                return
            raise BrokerError("接口不存在。", status=404)
        except BrokerError as err:
            self._send_json(err.status, {"ok": False, "code": err.code, "error": str(err)})
        except Exception as err:
            print(f"[hdhive-broker] GET {urllib.parse.urlparse(self.path).path} failed: {type(err).__name__}")
            self._send_json(500, {"ok": False, "error": "代理服务内部错误。"})

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[hdhive-broker] {self.command} {urllib.parse.urlparse(self.path).path}")


def _checkin_loop(stop_event: threading.Event) -> None:
    while not stop_event.wait(60):
        assert BROKER is not None
        for installation_id in BROKER.store.auto_checkin_installations():
            try:
                grant = BROKER.store.grant(installation_id)
                if "write" not in str(grant["scopes"] or "").split():
                    continue
                try:
                    from zoneinfo import ZoneInfo
                    local_date = datetime.now(ZoneInfo(str(grant["timezone_name"] or "Asia/Shanghai"))).date().isoformat()
                except Exception:
                    local_date = datetime.now(timezone.utc).date().isoformat()
                if grant["last_checkin_date"] != local_date:
                    result = BROKER.checkin(installation_id)
                    BROKER.store.save_checkin(installation_id, result, local_date)
            except Exception as err:
                print(f"[hdhive-broker] 自动签到失败 installation={installation_id[:12]} error={err}")


def main() -> None:
    global BROKER
    parser = argparse.ArgumentParser(description="Vistamirror HDHive OAuth broker")
    parser.add_argument("--host", default=str(os.environ.get("HDHIVE_BROKER_HOST") or "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("HDHIVE_BROKER_PORT") or 12388))
    args = parser.parse_args()
    BROKER = HDHiveBroker()
    stop_event = threading.Event()
    threading.Thread(target=_checkin_loop, args=(stop_event,), daemon=True).start()
    server = ThreadingHTTPServer((args.host, args.port), BrokerHandler)
    print(f"HDHive broker running at http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        server.server_close()


if __name__ == "__main__":
    main()
