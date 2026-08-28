from __future__ import annotations

import base64
import concurrent.futures
import hashlib
import http.client
import io
import json
import os
import pathlib
import re
import shlex
import socket
import threading
import time
import urllib.parse
import uuid
from datetime import datetime, timezone
from typing import Any, Callable


HOST_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
DOCKER_TARGET_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.:/@-]{0,255}$")
ALLOWED_CONTAINER_ACTIONS = {"start", "stop", "restart", "pause", "unpause"}
ALLOWED_COMPOSE_ACTIONS = {"deploy", "update", "start", "stop", "restart"}
LOCAL_DOCKER_HOST_ID = "local-docker"
DEFAULT_DOCKER_SOCKET_PATH = "/var/run/docker.sock"


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


class InfraError(RuntimeError):
    def __init__(self, message: str, *, status: int = 400, code: str = "infra_error") -> None:
        super().__init__(message)
        self.status = int(status)
        self.code = str(code)


def _format_bytes(value: Any) -> str:
    try:
        size = max(0, int(value or 0))
    except (TypeError, ValueError):
        return "—"
    units = ("B", "KB", "MB", "GB", "TB")
    index = 0
    while size >= 1024 and index < len(units) - 1:
        size /= 1024
        index += 1
    return f"{size:.0f} {units[index]}" if index == 0 or size >= 10 else f"{size:.1f} {units[index]}"


class _UnixSocketHTTPConnection(http.client.HTTPConnection):
    """HTTP transport for the local Docker Engine Unix socket."""

    def __init__(self, socket_path: str, timeout: int = 20) -> None:
        super().__init__("localhost", timeout=timeout)
        self.socket_path = socket_path

    def connect(self) -> None:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.settimeout(self.timeout)
        client.connect(self.socket_path)
        self.sock = client


class LocalDockerClient:
    """Small, dependency-free Docker Engine client for the mounted local socket."""

    def __init__(self, socket_path: str | None = None) -> None:
        self.socket_path = str(socket_path or os.environ.get("APP_DOCKER_SOCKET") or DEFAULT_DOCKER_SOCKET_PATH)

    @property
    def available(self) -> bool:
        return os.path.exists(self.socket_path)

    def _request(self, method: str, path: str, *, body: bytes | None = None, timeout: int = 30) -> tuple[int, dict[str, str], bytes]:
        if not self.available:
            raise InfraError(
                "未检测到本机 Docker Socket。请在 Compose 中挂载 /var/run/docker.sock:/var/run/docker.sock 后重建容器。",
                status=503,
                code="local_docker_socket_missing",
            )
        connection = _UnixSocketHTTPConnection(self.socket_path, timeout=max(3, min(int(timeout), 900)))
        try:
            connection.request(method, path, body=body, headers={"Content-Type": "application/json"} if body else {})
            response = connection.getresponse()
            payload = response.read(4 * 1024 * 1024)
            headers = {str(key).lower(): str(value) for key, value in response.getheaders()}
            status = int(response.status)
        except PermissionError as err:
            raise InfraError(
                "Docker Socket 无访问权限。请为 VistaMirror 设置可访问 Socket 的用户/组，或在受信任的内网环境以 root 运行该容器。",
                status=503,
                code="local_docker_socket_permission",
            ) from err
        except OSError as err:
            raise InfraError(f"无法连接本机 Docker Socket：{err}", status=502, code="local_docker_unavailable") from err
        finally:
            connection.close()
        if status >= 400:
            try:
                detail = str((json.loads(payload.decode("utf-8", errors="replace")) or {}).get("message") or "")
            except Exception:
                detail = payload.decode("utf-8", errors="replace").strip()
            raise InfraError(f"本机 Docker 返回 HTTP {status}{f'：{detail}' if detail else ''}", status=502, code="local_docker_api_failed")
        return status, headers, payload

    def _json(self, path: str, *, timeout: int = 30) -> Any:
        _status, _headers, payload = self._request("GET", path, timeout=timeout)
        try:
            return json.loads(payload.decode("utf-8", errors="replace"))
        except Exception as err:
            raise InfraError("本机 Docker 返回了无效数据。", status=502, code="local_docker_invalid_response") from err

    def info(self) -> dict[str, Any]:
        payload = self._json("/info")
        return dict(payload) if isinstance(payload, dict) else {}

    def version(self) -> dict[str, Any]:
        payload = self._json("/version")
        return dict(payload) if isinstance(payload, dict) else {}

    @staticmethod
    def _ports_text(ports: Any) -> str:
        values: list[str] = []
        for item in ports if isinstance(ports, list) else []:
            if not isinstance(item, dict):
                continue
            private = str(item.get("PrivatePort") or "")
            public = str(item.get("PublicPort") or "")
            kind = str(item.get("Type") or "tcp")
            values.append(f"{public + ':' if public else ''}{private}/{kind}" if private else "")
        return ", ".join(item for item in values if item) or "—"

    @staticmethod
    def _container_stats(payload: Any) -> dict[str, Any]:
        data = payload if isinstance(payload, dict) else {}
        cpu_stats = data.get("cpu_stats") if isinstance(data.get("cpu_stats"), dict) else {}
        pre_cpu_stats = data.get("precpu_stats") if isinstance(data.get("precpu_stats"), dict) else {}
        cpu_usage = cpu_stats.get("cpu_usage") if isinstance(cpu_stats.get("cpu_usage"), dict) else {}
        pre_cpu_usage = pre_cpu_stats.get("cpu_usage") if isinstance(pre_cpu_stats.get("cpu_usage"), dict) else {}
        cpu_delta = max(0, int(cpu_usage.get("total_usage") or 0) - int(pre_cpu_usage.get("total_usage") or 0))
        system_delta = max(0, int(cpu_stats.get("system_cpu_usage") or 0) - int(pre_cpu_stats.get("system_cpu_usage") or 0))
        online_cpus = int(cpu_stats.get("online_cpus") or len(cpu_usage.get("percpu_usage") or []) or 1)
        cpu_percent = (cpu_delta / system_delta * online_cpus * 100) if system_delta > 0 else 0.0

        memory_stats = data.get("memory_stats") if isinstance(data.get("memory_stats"), dict) else {}
        memory_detail = memory_stats.get("stats") if isinstance(memory_stats.get("stats"), dict) else {}
        raw_usage = max(0, int(memory_stats.get("usage") or 0))
        cache = max(0, int(memory_detail.get("inactive_file") or memory_detail.get("total_inactive_file") or memory_detail.get("cache") or 0))
        usage = max(0, raw_usage - min(raw_usage, cache))
        limit = max(0, int(memory_stats.get("limit") or 0))
        memory_percent = (usage / limit * 100) if limit > 0 else 0.0
        pids_stats = data.get("pids_stats") if isinstance(data.get("pids_stats"), dict) else {}
        return {
            "CPUPerc": f"{cpu_percent:.2f}%",
            "MemUsage": f"{_format_bytes(usage)} / {_format_bytes(limit)}" if limit else _format_bytes(usage),
            "MemPerc": f"{memory_percent:.2f}%",
            "MemoryUsageBytes": usage,
            "MemoryLimitBytes": limit,
            "PIDs": int(pids_stats.get("current") or 0),
        }

    def stats(self, containers: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        running = [row for row in containers if str(row.get("State") or "").lower() == "running"]
        if not running:
            return {}

        def fetch(row: dict[str, Any]) -> tuple[str, dict[str, Any]]:
            identifier = str(row.get("ID") or row.get("Names") or "")
            if not identifier:
                return "", {}
            payload = self._json(f"/containers/{urllib.parse.quote(identifier, safe='')}/stats?stream=false", timeout=20)
            return identifier, self._container_stats(payload)

        result: dict[str, dict[str, Any]] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(running))) as executor:
            futures = [executor.submit(fetch, row) for row in running]
            for future in concurrent.futures.as_completed(futures):
                try:
                    identifier, metrics = future.result()
                except Exception:
                    continue
                if identifier and metrics:
                    result[identifier] = metrics
        return result

    def containers(self) -> list[dict[str, Any]]:
        raw_containers = self._json("/containers/json?all=1")
        containers: list[dict[str, Any]] = []
        for item in raw_containers if isinstance(raw_containers, list) else []:
            if not isinstance(item, dict):
                continue
            names = item.get("Names") if isinstance(item.get("Names"), list) else []
            name = str(names[0] or "").lstrip("/") if names else str(item.get("Id") or "")[:12]
            containers.append({
                "ID": str(item.get("Id") or ""), "Names": name, "Image": str(item.get("Image") or ""),
                "State": str(item.get("State") or ""), "Status": str(item.get("Status") or ""),
                "Ports": self._ports_text(item.get("Ports")), "Labels": dict(item.get("Labels") or {}),
            })
        return containers

    def inventory(self) -> dict[str, Any]:
        containers = self.containers()
        raw_images = self._json("/images/json?all=0")
        images: list[dict[str, Any]] = []
        for item in raw_images if isinstance(raw_images, list) else []:
            if not isinstance(item, dict):
                continue
            tags = item.get("RepoTags") if isinstance(item.get("RepoTags"), list) else []
            for reference in tags or ["<none>:<none>"]:
                repository, separator, tag = str(reference).rpartition(":")
                images.append({
                    "Repository": repository if separator else str(reference), "Tag": tag if separator else "—",
                    "ID": str(item.get("Id") or ""), "Size": _format_bytes(item.get("Size")), "CreatedAt": str(item.get("Created" ) or ""),
                })
        compose_map: dict[str, dict[str, Any]] = {}
        for item in containers:
            labels = item.get("Labels") if isinstance(item.get("Labels"), dict) else {}
            project = str(labels.get("com.docker.compose.project") or "").strip()
            if not project:
                continue
            group = compose_map.setdefault(project, {"Name": project, "Status": [], "Containers": 0, "Source": "Docker Socket"})
            group["Containers"] = int(group["Containers"]) + 1
            group["Status"].append(str(item.get("State") or "unknown"))
        compose = [{**row, "Status": ", ".join(sorted(set(row.pop("Status"))))} for row in compose_map.values()]
        return {"containers": containers, "images": images, "compose": compose}

    def logs(self, container: str, *, tail: int) -> str:
        encoded = urllib.parse.quote(container, safe="")
        _status, headers, payload = self._request("GET", f"/containers/{encoded}/logs?stdout=1&stderr=1&timestamps=1&tail={tail}", timeout=45)
        if "application/vnd.docker.raw-stream" in headers.get("content-type", ""):
            chunks: list[bytes] = []
            offset = 0
            while offset + 8 <= len(payload):
                length = int.from_bytes(payload[offset + 4:offset + 8], "big")
                offset += 8
                chunks.append(payload[offset:offset + length])
                offset += length
            payload = b"".join(chunks) if chunks else payload
        return payload.decode("utf-8", errors="replace")

    def container_action(self, container: str, action: str) -> dict[str, Any]:
        encoded = urllib.parse.quote(container, safe="")
        suffix = "?t=20" if action in {"stop", "restart"} else ""
        self._request("POST", f"/containers/{encoded}/{action}{suffix}", timeout=90)
        return {"exitCode": 0, "output": f"本机容器 {container} 已执行 {action}"}

    def pull_image(self, image: str) -> dict[str, Any]:
        _status, _headers, payload = self._request("POST", f"/images/create?fromImage={urllib.parse.quote(image, safe='/:@')}", timeout=900)
        lines = [line for line in payload.decode("utf-8", errors="replace").splitlines() if line.strip()]
        messages: list[str] = []
        for line in lines[-20:]:
            try:
                data = json.loads(line)
                messages.append(str(data.get("status") or data.get("error") or line))
            except Exception:
                messages.append(line)
        return {"exitCode": 0, "output": "\n".join(messages[-12:]) or f"镜像 {image} 拉取完成"}


class InfraCredentialCipher:
    """AES-GCM envelope for credentials kept under APP_DATA_DIR."""

    def __init__(self, secret: str | None = None) -> None:
        raw = str(secret if secret is not None else os.environ.get("APP_INFRA_MASTER_KEY") or "").strip()
        self._key = hashlib.sha256(raw.encode("utf-8")).digest() if raw else None

    @property
    def available(self) -> bool:
        return self._key is not None

    def encrypt(self, value: str) -> str:
        text = str(value or "")
        if not text:
            return ""
        if self._key is None:
            raise InfraError(
                "保存 SSH 密码或私钥前，请先配置 APP_INFRA_MASTER_KEY。",
                code="master_key_required",
            )
        try:
            from Crypto.Cipher import AES
        except Exception as err:  # pragma: no cover - deployment dependency
            raise InfraError(f"凭据加密组件不可用：{err}", status=500, code="crypto_unavailable") from err
        cipher = AES.new(self._key, AES.MODE_GCM)
        ciphertext, tag = cipher.encrypt_and_digest(text.encode("utf-8"))
        envelope = b"VM1" + cipher.nonce + tag + ciphertext
        return base64.urlsafe_b64encode(envelope).decode("ascii")

    def decrypt(self, value: str) -> str:
        encoded = str(value or "")
        if not encoded:
            return ""
        if self._key is None:
            raise InfraError(
                "SSH 凭据已加密，但当前未配置 APP_INFRA_MASTER_KEY。",
                status=503,
                code="master_key_missing",
            )
        try:
            from Crypto.Cipher import AES

            envelope = base64.urlsafe_b64decode(encoded.encode("ascii"))
            if not envelope.startswith(b"VM1") or len(envelope) < 35:
                raise ValueError("invalid envelope")
            nonce = envelope[3:19]
            tag = envelope[19:35]
            ciphertext = envelope[35:]
            cipher = AES.new(self._key, AES.MODE_GCM, nonce=nonce)
            return cipher.decrypt_and_verify(ciphertext, tag).decode("utf-8")
        except InfraError:
            raise
        except Exception as err:
            raise InfraError("SSH 凭据无法解密，请检查 APP_INFRA_MASTER_KEY。", status=503, code="credential_decrypt_failed") from err


class ParamikoSshRunner:
    def __init__(self, host: dict[str, Any], cipher: InfraCredentialCipher, *, known_hosts_file: pathlib.Path) -> None:
        self.host = host
        self.cipher = cipher
        self.known_hosts_file = known_hosts_file

    @staticmethod
    def _private_key(paramiko: Any, content: str, passphrase: str = "") -> Any:
        last_error: Exception | None = None
        for key_type in (paramiko.Ed25519Key, paramiko.RSAKey, paramiko.ECDSAKey):
            try:
                return key_type.from_private_key(io.StringIO(content), password=passphrase or None)
            except Exception as err:  # pragma: no cover - depends on key type
                last_error = err
        raise InfraError(f"无法读取 SSH 私钥：{last_error or '未知格式'}", code="invalid_private_key")

    def _connect(self) -> tuple[Any, str]:
        try:
            import paramiko
        except Exception as err:
            raise InfraError(
                "当前环境缺少 Paramiko，请更新 Docker 镜像或安装 requirements.txt 依赖。",
                status=503,
                code="paramiko_unavailable",
            ) from err

        address = str(self.host.get("address") or "").strip()
        username = str(self.host.get("username") or "").strip()
        if not address or not username:
            raise InfraError("服务器地址和 SSH 用户名不能为空。", code="invalid_host")
        port = max(1, min(65535, int(self.host.get("port") or 22)))
        auth_mode = str(self.host.get("authMode") or "agent")
        client = paramiko.SSHClient()
        self.known_hosts_file.parent.mkdir(parents=True, exist_ok=True)
        if self.known_hosts_file.exists():
            client.load_host_keys(str(self.known_hosts_file))
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        kwargs: dict[str, Any] = {
            "hostname": address,
            "port": port,
            "username": username,
            "timeout": 10,
            "banner_timeout": 10,
            "auth_timeout": 10,
            "allow_agent": auth_mode == "agent",
            "look_for_keys": auth_mode == "agent",
        }
        if auth_mode == "password":
            kwargs["password"] = self.cipher.decrypt(str(self.host.get("passwordEncrypted") or ""))
            kwargs["allow_agent"] = False
            kwargs["look_for_keys"] = False
        elif auth_mode == "private_key":
            private_key = self.cipher.decrypt(str(self.host.get("privateKeyEncrypted") or ""))
            passphrase = self.cipher.decrypt(str(self.host.get("privateKeyPassphraseEncrypted") or ""))
            kwargs["pkey"] = self._private_key(paramiko, private_key, passphrase)
            kwargs["allow_agent"] = False
            kwargs["look_for_keys"] = False
        elif auth_mode == "key_path":
            kwargs["key_filename"] = str(self.host.get("keyPath") or "").strip()
            kwargs["allow_agent"] = False
            kwargs["look_for_keys"] = False
        try:
            client.connect(**kwargs)
            client.save_host_keys(str(self.known_hosts_file))
            remote_key = client.get_transport().get_remote_server_key()
            fingerprint = "SHA256:" + base64.b64encode(hashlib.sha256(remote_key.asbytes()).digest()).decode("ascii").rstrip("=")
            expected = str(self.host.get("fingerprint") or "").strip()
            if expected and expected != fingerprint:
                client.close()
                raise InfraError("SSH 主机指纹与已保存记录不一致，已拒绝连接。", status=409, code="host_key_changed")
            return client, fingerprint
        except InfraError:
            raise
        except Exception as err:
            client.close()
            raise InfraError(f"SSH 连接失败：{err}", status=502, code="ssh_connect_failed") from err

    def run(self, command: str, *, timeout: int = 45) -> dict[str, Any]:
        client, fingerprint = self._connect()
        try:
            command_timeout = max(3, min(int(timeout), 900))
            _stdin, stdout, stderr = client.exec_command(command, timeout=command_timeout, get_pty=False)
            channel = stdout.channel
            output_chunks: list[bytes] = []
            error_chunks: list[bytes] = []
            output_size = 0
            error_size = 0
            deadline = time.monotonic() + command_timeout
            while True:
                while channel.recv_ready():
                    chunk = channel.recv(65536)
                    if not chunk:
                        break
                    if output_size < 1024 * 1024:
                        output_chunks.append(chunk[: 1024 * 1024 - output_size])
                        output_size += len(output_chunks[-1])
                while channel.recv_stderr_ready():
                    chunk = channel.recv_stderr(65536)
                    if not chunk:
                        break
                    if error_size < 512 * 1024:
                        error_chunks.append(chunk[: 512 * 1024 - error_size])
                        error_size += len(error_chunks[-1])
                if channel.exit_status_ready() and not channel.recv_ready() and not channel.recv_stderr_ready():
                    break
                if time.monotonic() >= deadline:
                    channel.close()
                    raise InfraError("SSH 命令执行超时。", status=504, code="ssh_command_timeout")
                time.sleep(0.02)
            exit_code = int(channel.recv_exit_status())
            output = b"".join(output_chunks).decode("utf-8", errors="replace")
            error = b"".join(error_chunks).decode("utf-8", errors="replace")
            return {"exitCode": exit_code, "stdout": output, "stderr": error, "fingerprint": fingerprint}
        except Exception as err:
            raise InfraError(f"SSH 命令执行失败：{err}", status=502, code="ssh_command_failed") from err
        finally:
            client.close()


class InfraOperationManager:
    def __init__(self, path: pathlib.Path, *, event_logger: Callable[..., None] | None = None) -> None:
        self.path = path
        self.event_logger = event_logger
        self._lock = threading.RLock()
        self._target_locks: dict[str, threading.Lock] = {}
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="infra-operation")

    def _load(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return []
        return payload if isinstance(payload, list) else []

    def _save(self, rows: list[dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(".tmp")
        temp.write_text(json.dumps(rows[-300:], ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(self.path)

    def list(self, *, limit: int = 80) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._load()
        return list(reversed(rows[-max(1, min(int(limit), 300)):]))

    def _update(self, operation_id: str, **changes: Any) -> dict[str, Any]:
        with self._lock:
            rows = self._load()
            current: dict[str, Any] | None = None
            for row in rows:
                if str(row.get("id")) == operation_id:
                    row.update(changes)
                    current = dict(row)
                    break
            if current is None:
                raise InfraError("操作记录不存在。", status=404, code="operation_not_found")
            self._save(rows)
            return current

    def submit(
        self,
        *,
        host_id: str,
        action: str,
        target: str,
        description: str,
        callback: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        operation = {
            "id": uuid.uuid4().hex,
            "hostId": host_id,
            "action": action,
            "target": target,
            "description": description,
            "status": "queued",
            "createdAt": _now_iso(),
            "startedAt": "",
            "finishedAt": "",
            "result": {},
            "error": "",
        }
        with self._lock:
            rows = self._load()
            rows.append(operation)
            self._save(rows)
            target_lock = self._target_locks.setdefault(host_id, threading.Lock())

        def run() -> None:
            with target_lock:
                self._update(operation["id"], status="running", startedAt=_now_iso())
                try:
                    result = callback()
                    self._update(operation["id"], status="success", finishedAt=_now_iso(), result=result, error="")
                    if self.event_logger:
                        self.event_logger(level="info", module="docker", action=action, message=description, status=200, detail={"hostId": host_id, "target": target})
                except Exception as err:
                    self._update(operation["id"], status="failed", finishedAt=_now_iso(), error=str(err)[:800])
                    if self.event_logger:
                        self.event_logger(level="error", module="docker", action=action, message=f"{description}失败。", status=500, detail={"hostId": host_id, "target": target, "error": str(err)[:500]})

        self._executor.submit(run)
        return operation


class InfraService:
    def __init__(
        self,
        *,
        data_dir: pathlib.Path,
        event_logger: Callable[..., None] | None = None,
        runner_factory: Callable[[dict[str, Any]], Any] | None = None,
        master_key: str | None = None,
    ) -> None:
        self.data_dir = pathlib.Path(data_dir)
        self.config_file = self.data_dir / "infra_config.json"
        self.known_hosts_file = self.data_dir / "infra_known_hosts"
        self.cipher = InfraCredentialCipher(master_key)
        self.event_logger = event_logger
        self.runner_factory = runner_factory
        self._lock = threading.RLock()
        self.operations = InfraOperationManager(self.data_dir / "infra_operations.json", event_logger=event_logger)

    @staticmethod
    def _default_config() -> dict[str, Any]:
        return {
            "hosts": [],
            "projects": [],
        }

    @staticmethod
    def _local_host() -> dict[str, Any]:
        return {
            "id": LOCAL_DOCKER_HOST_ID,
            "name": "本机 Docker",
            "address": "unix:///var/run/docker.sock",
            "port": 0,
            "username": "Docker Engine",
            "authMode": "socket",
            "keyPath": "",
            "fingerprint": "",
            "group": "本机",
            "tags": ["Docker", "自动发现"],
            "enabled": True,
            "local": True,
            "createdAt": "",
            "updatedAt": "",
        }

    @staticmethod
    def _is_local_host_id(host_id: str) -> bool:
        return str(host_id or "").strip() == LOCAL_DOCKER_HOST_ID

    def _local_docker(self) -> LocalDockerClient:
        return LocalDockerClient()

    def _load(self) -> dict[str, Any]:
        if not self.config_file.exists():
            return self._default_config()
        try:
            payload = json.loads(self.config_file.read_text(encoding="utf-8"))
        except Exception:
            return self._default_config()
        config = self._default_config()
        if isinstance(payload, dict):
            for key in config:
                if key in payload:
                    config[key] = payload[key]
        return config

    def _save(self, config: dict[str, Any]) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        temp = self.config_file.with_suffix(".tmp")
        temp.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(self.config_file)

    @staticmethod
    def _public_host(host: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": str(host.get("id") or ""),
            "name": str(host.get("name") or ""),
            "address": str(host.get("address") or ""),
            "port": int(host.get("port") or 22),
            "username": str(host.get("username") or ""),
            "authMode": str(host.get("authMode") or "agent"),
            "keyPath": str(host.get("keyPath") or ""),
            "fingerprint": str(host.get("fingerprint") or ""),
            "group": str(host.get("group") or "默认"),
            "tags": [str(tag) for tag in (host.get("tags") or []) if str(tag).strip()],
            "enabled": bool(host.get("enabled", True)),
            "hasPassword": bool(host.get("passwordEncrypted")),
            "hasPrivateKey": bool(host.get("privateKeyEncrypted")),
            "createdAt": str(host.get("createdAt") or ""),
            "updatedAt": str(host.get("updatedAt") or ""),
        }

    def public_config(self) -> dict[str, Any]:
        with self._lock:
            config = self._load()
        saved_hosts = [self._public_host(host) for host in config.get("hosts") or [] if isinstance(host, dict)]
        return {
            "hosts": [self._public_host(self._local_host())] + [host for host in saved_hosts if host.get("id") != LOCAL_DOCKER_HOST_ID],
            "projects": [dict(item) for item in config.get("projects") or [] if isinstance(item, dict)],
            "credentialEncryptionReady": self.cipher.available,
        }

    def _host(self, host_id: str) -> dict[str, Any]:
        target = str(host_id or "").strip()
        if self._is_local_host_id(target):
            return self._local_host()
        with self._lock:
            for host in self._load().get("hosts") or []:
                if isinstance(host, dict) and str(host.get("id")) == target:
                    return dict(host)
        raise InfraError("服务器不存在。", status=404, code="host_not_found")

    def _runner(self, host: dict[str, Any]) -> Any:
        if self.runner_factory:
            return self.runner_factory(host)
        return ParamikoSshRunner(host, self.cipher, known_hosts_file=self.known_hosts_file)

    def save_host(self, payload: dict[str, Any]) -> dict[str, Any]:
        host_id = str(payload.get("id") or uuid.uuid4().hex[:12]).strip()
        if self._is_local_host_id(host_id):
            raise InfraError("“本机 Docker”由系统自动发现，无需手动保存。", code="local_host_managed")
        if not HOST_ID_PATTERN.match(host_id):
            raise InfraError("服务器 ID 格式不正确。", code="invalid_host_id")
        address = str(payload.get("address") or "").strip()
        username = str(payload.get("username") or "").strip()
        if not address or not username:
            raise InfraError("服务器地址和 SSH 用户名不能为空。", code="invalid_host")
        try:
            port = int(payload.get("port") or 22)
        except (TypeError, ValueError):
            raise InfraError("SSH 端口格式不正确。", code="invalid_port")
        if not 1 <= port <= 65535:
            raise InfraError("SSH 端口超出有效范围。", code="invalid_port")
        auth_mode = str(payload.get("authMode") or "agent").strip()
        if auth_mode not in {"agent", "password", "private_key", "key_path"}:
            raise InfraError("不支持的 SSH 认证方式。", code="invalid_auth_mode")
        now = _now_iso()
        with self._lock:
            config = self._load()
            hosts = [dict(item) for item in config.get("hosts") or [] if isinstance(item, dict)]
            existing = next((item for item in hosts if str(item.get("id")) == host_id), {})
            host = {
                **existing,
                "id": host_id,
                "name": str(payload.get("name") or existing.get("name") or address).strip(),
                "address": address,
                "port": port,
                "username": username,
                "authMode": auth_mode,
                "keyPath": str(payload.get("keyPath") or existing.get("keyPath") or "").strip(),
                "group": str(payload.get("group") or existing.get("group") or "默认").strip() or "默认",
                "tags": [str(tag).strip() for tag in (payload.get("tags") or existing.get("tags") or []) if str(tag).strip()][:20],
                "enabled": bool(payload.get("enabled", existing.get("enabled", True))),
                "fingerprint": str(existing.get("fingerprint") or ""),
                "createdAt": str(existing.get("createdAt") or now),
                "updatedAt": now,
            }
            for plain_key, encrypted_key in (
                ("password", "passwordEncrypted"),
                ("privateKey", "privateKeyEncrypted"),
                ("privateKeyPassphrase", "privateKeyPassphraseEncrypted"),
            ):
                if plain_key in payload and str(payload.get(plain_key) or ""):
                    host[encrypted_key] = self.cipher.encrypt(str(payload.get(plain_key) or ""))
                elif payload.get(f"clear{plain_key[0].upper()}{plain_key[1:]}"):
                    host[encrypted_key] = ""
            hosts = [item for item in hosts if str(item.get("id")) != host_id] + [host]
            config["hosts"] = hosts
            self._save(config)
        return self._public_host(host)

    def delete_host(self, host_id: str) -> None:
        target = str(host_id or "").strip()
        if self._is_local_host_id(target):
            raise InfraError("“本机 Docker”由 Docker Socket 挂载状态决定，不能在页面中删除。", code="local_host_managed")
        with self._lock:
            config = self._load()
            original = config.get("hosts") or []
            hosts = [item for item in original if not isinstance(item, dict) or str(item.get("id")) != target]
            if len(hosts) == len(original):
                raise InfraError("服务器不存在。", status=404, code="host_not_found")
            config["hosts"] = hosts
            config["projects"] = [item for item in config.get("projects") or [] if not isinstance(item, dict) or str(item.get("hostId")) != target]
            self._save(config)

    def test_host(self, host_id: str) -> dict[str, Any]:
        if self._is_local_host_id(host_id):
            status = self.host_status(host_id)
            return {"ok": True, "hostname": str(status.get("hostname") or "本机 Docker"), "fingerprint": "Docker Socket"}
        host = self._host(host_id)
        result = self._runner(host).run("printf 'connected\\n'; hostname", timeout=15)
        if int(result.get("exitCode") or 0) != 0:
            raise InfraError(str(result.get("stderr") or "SSH 测试失败。"), status=502, code="ssh_test_failed")
        fingerprint = str(result.get("fingerprint") or "")
        if fingerprint and fingerprint != str(host.get("fingerprint") or ""):
            with self._lock:
                config = self._load()
                for item in config.get("hosts") or []:
                    if isinstance(item, dict) and str(item.get("id")) == host_id:
                        item["fingerprint"] = fingerprint
                        item["updatedAt"] = _now_iso()
                self._save(config)
        lines = [line.strip() for line in str(result.get("stdout") or "").splitlines() if line.strip()]
        return {"ok": True, "hostname": lines[-1] if lines else "", "fingerprint": fingerprint}

    @staticmethod
    def _parse_key_values(text: str) -> dict[str, str]:
        result: dict[str, str] = {}
        for line in str(text or "").splitlines():
            if "\t" not in line:
                continue
            key, value = line.split("\t", 1)
            result[key.strip()] = value.strip()
        return result

    def host_status(self, host_id: str) -> dict[str, Any]:
        if self._is_local_host_id(host_id):
            info = self._local_docker().info()
            version = self._local_docker().version()
            memory_total = int(info.get("MemTotal") or 0)
            return {
                "hostId": LOCAL_DOCKER_HOST_ID,
                "online": True,
                "hostname": str(info.get("Name") or "本机 Docker"),
                "kernel": str(info.get("KernelVersion") or "Docker Engine"),
                "cpuCount": int(info.get("NCPU") or 0),
                "load": [], "uptimeSeconds": 0,
                "memory": {"total": memory_total, "available": 0, "used": 0},
                "disk": {"total": 0, "used": 0, "available": 0},
                "dockerVersion": str(version.get("Version") or info.get("ServerVersion") or ""),
                "composeVersion": "Docker Socket",
                "containerCount": int(info.get("Containers") or 0),
                "checkedAt": _now_iso(),
            }
        host = self._host(host_id)
        command = """printf 'hostname\\t'; hostname; printf 'kernel\\t'; uname -srm; printf 'cpuCount\\t'; getconf _NPROCESSORS_ONLN 2>/dev/null || nproc; printf 'load\\t'; cut -d' ' -f1-3 /proc/loadavg; printf 'uptime\\t'; cut -d' ' -f1 /proc/uptime; awk '/MemTotal:/{t=$2*1024}/MemAvailable:/{a=$2*1024}END{printf \"memory\\t%.0f %.0f\\n\",t,a}' /proc/meminfo; df -B1 -P / | awk 'NR==2{printf \"disk\\t%s %s %s\\n\",$2,$3,$4}'; printf 'docker\\t'; docker version --format '{{.Server.Version}}' 2>/dev/null || true; printf 'compose\\t'; docker compose version --short 2>/dev/null || true"""
        result = self._runner(host).run(command, timeout=20)
        if int(result.get("exitCode") or 0) != 0:
            raise InfraError(str(result.get("stderr") or "读取服务器状态失败。"), status=502, code="status_failed")
        values = self._parse_key_values(str(result.get("stdout") or ""))
        mem = [int(float(value)) for value in values.get("memory", "0 0").split()[:2]]
        while len(mem) < 2:
            mem.append(0)
        disk = [int(float(value)) for value in values.get("disk", "0 0 0").split()[:3]]
        while len(disk) < 3:
            disk.append(0)
        return {
            "hostId": host_id,
            "online": True,
            "hostname": values.get("hostname", ""),
            "kernel": values.get("kernel", ""),
            "cpuCount": int(float(values.get("cpuCount") or 0)),
            "load": [float(item) for item in values.get("load", "0 0 0").split()[:3]],
            "uptimeSeconds": int(float(values.get("uptime") or 0)),
            "memory": {"total": mem[0], "available": mem[1], "used": max(0, mem[0] - mem[1])},
            "disk": {"total": disk[0], "used": disk[1], "available": disk[2]},
            "dockerVersion": values.get("docker", ""),
            "composeVersion": values.get("compose", ""),
            "checkedAt": _now_iso(),
        }

    @staticmethod
    def _json_rows(text: str) -> list[dict[str, Any]]:
        raw = str(text or "").strip()
        if not raw:
            return []
        try:
            payload = json.loads(raw)
            if isinstance(payload, list):
                return [dict(item) for item in payload if isinstance(item, dict)]
            if isinstance(payload, dict):
                return [dict(payload)]
        except Exception:
            pass
        rows: list[dict[str, Any]] = []
        for line in raw.splitlines():
            try:
                value = json.loads(line)
            except Exception:
                continue
            if isinstance(value, dict):
                rows.append(dict(value))
        return rows

    def docker_inventory(self, host_id: str) -> dict[str, Any]:
        if self._is_local_host_id(host_id):
            inventory = self._local_docker().inventory()
            return {"hostId": LOCAL_DOCKER_HOST_ID, **inventory, "checkedAt": _now_iso()}
        host = self._host(host_id)
        runner = self._runner(host)
        containers_result = runner.run("docker ps -a --no-trunc --format '{{json .}}'", timeout=30)
        images_result = runner.run("docker image ls --no-trunc --format '{{json .}}'", timeout=30)
        compose_result = runner.run("docker compose ls --format json", timeout=30)
        for result in (containers_result, images_result, compose_result):
            if int(result.get("exitCode") or 0) != 0:
                raise InfraError(str(result.get("stderr") or "Docker 查询失败。"), status=502, code="docker_query_failed")
        return {
            "hostId": host_id,
            "containers": self._json_rows(str(containers_result.get("stdout") or "")),
            "images": self._json_rows(str(images_result.get("stdout") or "")),
            "compose": self._json_rows(str(compose_result.get("stdout") or "")),
            "checkedAt": _now_iso(),
        }

    def docker_stats(self, host_id: str) -> dict[str, Any]:
        if self._is_local_host_id(host_id):
            client = self._local_docker()
            containers = client.containers()
            metrics = client.stats(containers)
            rows = [{"ID": identifier, **values} for identifier, values in metrics.items()]
            return {"hostId": LOCAL_DOCKER_HOST_ID, "stats": rows, "checkedAt": _now_iso()}
        result = self._runner(self._host(host_id)).run("docker stats --no-stream --format '{{json .}}'", timeout=45)
        if int(result.get("exitCode") or 0) != 0:
            raise InfraError(str(result.get("stderr") or "Docker 资源统计读取失败。"), status=502, code="docker_stats_failed")
        return {
            "hostId": host_id,
            "stats": self._json_rows(str(result.get("stdout") or "")),
            "checkedAt": _now_iso(),
        }

    def container_logs(self, host_id: str, container: str, *, tail: int = 300) -> dict[str, Any]:
        if not DOCKER_TARGET_PATTERN.match(str(container or "")):
            raise InfraError("容器标识格式不正确。", code="invalid_container")
        count = max(10, min(int(tail), 2000))
        if self._is_local_host_id(host_id):
            return {"hostId": LOCAL_DOCKER_HOST_ID, "container": container, "logs": self._local_docker().logs(container, tail=count), "exitCode": 0}
        result = self._runner(self._host(host_id)).run(f"docker logs --tail {count} --timestamps {shlex.quote(container)} 2>&1", timeout=45)
        return {"hostId": host_id, "container": container, "logs": str(result.get("stdout") or ""), "exitCode": int(result.get("exitCode") or 0)}

    def submit_container_action(self, host_id: str, container: str, action: str) -> dict[str, Any]:
        if action not in ALLOWED_CONTAINER_ACTIONS:
            raise InfraError("不支持的容器操作。", code="invalid_action")
        if not DOCKER_TARGET_PATTERN.match(str(container or "")):
            raise InfraError("容器标识格式不正确。", code="invalid_container")
        if self._is_local_host_id(host_id):
            callback = lambda: self._local_docker().container_action(container, action)
        else:
            option = " --time 20" if action in {"stop", "restart"} else ""
            command = f"docker container {action}{option} {shlex.quote(container)}"
            callback = lambda: self._checked_run(host_id, command, timeout=90)
        return self.operations.submit(
            host_id=host_id,
            action=f"container_{action}",
            target=container,
            description=f"容器 {container} 执行 {action}",
            callback=callback,
        )

    def submit_image_pull(self, host_id: str, image: str) -> dict[str, Any]:
        if not DOCKER_TARGET_PATTERN.match(str(image or "")):
            raise InfraError("镜像名称格式不正确。", code="invalid_image")
        callback = (lambda: self._local_docker().pull_image(image)) if self._is_local_host_id(host_id) else (lambda: self._checked_run(host_id, f"docker image pull {shlex.quote(image)}", timeout=900))
        return self.operations.submit(
            host_id=host_id,
            action="image_pull",
            target=image,
            description=f"拉取镜像 {image}",
            callback=callback,
        )

    def save_project(self, payload: dict[str, Any]) -> dict[str, Any]:
        project_id = str(payload.get("id") or uuid.uuid4().hex[:12]).strip()
        if not HOST_ID_PATTERN.match(project_id):
            raise InfraError("Compose 项目 ID 格式不正确。", code="invalid_project_id")
        host_id = str(payload.get("hostId") or "").strip()
        if self._is_local_host_id(host_id):
            raise InfraError("本机 Docker 会自动识别 Compose 项目；部署或更新请使用远程 SSH 主机配置。", code="local_compose_auto_discovered")
        self._host(host_id)
        name = str(payload.get("name") or "").strip()
        compose_path = str(payload.get("composePath") or "").strip()
        if not name or not compose_path.startswith("/"):
            raise InfraError("Compose 项目名称不能为空，配置路径必须是绝对路径。", code="invalid_project")
        project = {
            "id": project_id,
            "hostId": host_id,
            "name": name,
            "composePath": compose_path,
            "group": str(payload.get("group") or "默认").strip() or "默认",
            "tags": [str(tag).strip() for tag in (payload.get("tags") or []) if str(tag).strip()][:20],
            "updatedAt": _now_iso(),
        }
        with self._lock:
            config = self._load()
            config["projects"] = [item for item in config.get("projects") or [] if not isinstance(item, dict) or str(item.get("id")) != project_id] + [project]
            self._save(config)
        return project

    def delete_project(self, project_id: str) -> None:
        with self._lock:
            config = self._load()
            rows = config.get("projects") or []
            updated = [item for item in rows if not isinstance(item, dict) or str(item.get("id")) != str(project_id)]
            if len(updated) == len(rows):
                raise InfraError("Compose 项目不存在。", status=404, code="project_not_found")
            config["projects"] = updated
            self._save(config)

    def _project(self, project_id: str) -> dict[str, Any]:
        with self._lock:
            for item in self._load().get("projects") or []:
                if isinstance(item, dict) and str(item.get("id")) == str(project_id):
                    return dict(item)
        raise InfraError("Compose 项目不存在。", status=404, code="project_not_found")

    def submit_compose_action(self, project_id: str, action: str) -> dict[str, Any]:
        if action not in ALLOWED_COMPOSE_ACTIONS:
            raise InfraError("不支持的 Compose 操作。", code="invalid_action")
        project = self._project(project_id)
        compose = f"docker compose -f {shlex.quote(str(project['composePath']))} -p {shlex.quote(str(project['name']))}"
        commands = {
            "deploy": f"{compose} config -q && {compose} up -d --remove-orphans",
            "update": f"{compose} config -q && {compose} pull && {compose} up -d --remove-orphans",
            "start": f"{compose} start",
            "stop": f"{compose} stop",
            "restart": f"{compose} restart",
        }
        return self.operations.submit(
            host_id=str(project["hostId"]),
            action=f"compose_{action}",
            target=project_id,
            description=f"Compose 项目 {project['name']} 执行 {action}",
            callback=lambda: self._checked_run(str(project["hostId"]), commands[action], timeout=900),
        )

    def _checked_run(self, host_id: str, command: str, *, timeout: int) -> dict[str, Any]:
        result = self._runner(self._host(host_id)).run(command, timeout=timeout)
        if int(result.get("exitCode") or 0) != 0:
            raise InfraError(str(result.get("stderr") or result.get("stdout") or "远程操作执行失败。"), status=502, code="remote_action_failed")
        return {"exitCode": 0, "output": str(result.get("stdout") or "")[-12000:]}
