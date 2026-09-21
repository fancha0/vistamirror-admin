from __future__ import annotations

import base64
import concurrent.futures
import copy
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
import urllib.request
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


def _demux_docker_log_stream(payload: bytes) -> bytes:
    """Strip Docker's 8-byte multiplex frame headers from /containers/{id}/logs output.

    Older code relied on the Content-Type header, but some daemons/proxies omit
    it, leaking header bytes into the visible log (the stray char before each
    line). Detect framing structurally instead: a valid stream is an exact
    sequence of [stream_byte, 0, 0, 0, len(4B BE)] + len bytes frames.
    TTY containers return raw text without frames; validation keeps it intact.
    """
    if len(payload) < 8 or payload[0] not in (0, 1, 2) or payload[1:4] != b"\x00\x00\x00":
        return payload
    chunks: list[bytes] = []
    offset = 0
    while offset + 8 <= len(payload):
        if payload[offset] not in (0, 1, 2) or payload[offset + 1:offset + 4] != b"\x00\x00\x00":
            return payload  # 结构不符，按原始文本返回
        length = int.from_bytes(payload[offset + 4:offset + 8], "big")
        frame_end = offset + 8 + length
        if frame_end > len(payload):
            return payload
        chunks.append(payload[offset + 8:frame_end])
        offset = frame_end
    if offset != len(payload) or not chunks:
        return payload
    return b"".join(chunks)


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


# ---- Docker Hub 镜像更新检测 ----

_REGISTRY_URLOPEN: Callable[..., Any] = urllib.request.urlopen


def set_registry_urlopen(fn: Callable[..., Any]) -> None:
    """让 dev_server 注入带全局代理的 urlopen（国内网络访问 Docker Hub 常需要）。"""
    global _REGISTRY_URLOPEN
    _REGISTRY_URLOPEN = fn


def _split_image_ref(image: str) -> tuple[str, str] | None:
    """把镜像引用拆成 (repo, tag)；只看 Docker Hub，其他登记处返回 None。"""
    ref = str(image or "").strip()
    if not ref or "@" in ref:
        return None
    if "/" in ref:
        first, rest = ref.split("/", 1)
        if first in {"docker.io", "index.docker.io", "registry-1.docker.io"}:
            ref = rest
        elif "." in first or ":" in first or first == "localhost":
            return None
    if ":" in ref.split("/")[-1]:
        repo, tag = ref.rsplit(":", 1)
    else:
        repo, tag = ref, "latest"
    if "/" not in repo:
        repo = f"library/{repo}"
    return repo, tag


def _docker_hub_remote_digest(repo: str, tag: str) -> set[str]:
    """通过 Docker Hub Registry API 拿 tag 当前指向的 digest；失败由调用方标记为未知。"""
    token_req = urllib.request.Request(
        f"https://auth.docker.io/token?service=registry.docker.io&scope=repository:{urllib.parse.quote(repo)}:pull",
        headers={"Accept": "application/json", "User-Agent": "Vistamirror/1.0"},
    )
    with _REGISTRY_URLOPEN(token_req, timeout=15) as response:
        token = str(json.loads(response.read().decode("utf-8", errors="replace")).get("token") or "")
    if not token:
        return set()
    manifest_req = urllib.request.Request(
        f"https://registry-1.docker.io/v2/{repo}/manifests/{urllib.parse.quote(tag)}",
        headers={
            "Authorization": f"Bearer {token}",
            # 同时接受 manifest 和 manifest list，digest 以返回头为准
            "Accept": ", ".join([
                "application/vnd.oci.image.index.v1+json",
                "application/vnd.docker.distribution.manifest.list.v2+json",
                "application/vnd.docker.distribution.manifest.v2+json",
                "application/vnd.oci.image.manifest.v1+json",
            ]),
            "User-Agent": "Vistamirror/1.0",
        },
    )
    with _REGISTRY_URLOPEN(manifest_req, timeout=15) as response:
        digest = response.headers.get("Docker-Content-Digest")
        digests = {str(digest).strip()} if digest else set()
        manifest = json.loads(response.read().decode("utf-8"))
        # 多架构镜像的 RepoDigest 可能是 index，也可能是平台 manifest。
        for item in manifest.get("manifests") or []:
            if item.get("digest"):
                digests.add(str(item["digest"]))
        return digests


def _local_image_digest(repo_digests: Any, repo: str) -> str:
    """从镜像的 RepoDigests 里挑出指定 repo 的 digest。"""
    if not isinstance(repo_digests, list):
        return ""
    for entry in repo_digests:
        text = str(entry or "")
        if "@" not in text:
            continue
        name, digest = text.split("@", 1)
        if _split_image_ref(name) == (repo, "latest"):
            return digest.strip()
    return ""


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
                "ImageID": str(item.get("ImageID") or ""),
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
        _status, _headers, payload = self._request("GET", f"/containers/{encoded}/logs?stdout=1&stderr=1&timestamps=1&tail={tail}", timeout=45)
        return _demux_docker_log_stream(payload).decode("utf-8", errors="replace")

    def container_action(self, container: str, action: str) -> dict[str, Any]:
        encoded = urllib.parse.quote(container, safe="")
        suffix = "?t=20" if action in {"stop", "restart"} else ""
        self._request("POST", f"/containers/{encoded}/{action}{suffix}", timeout=90)
        return {"exitCode": 0, "output": f"本机容器 {container} 已执行 {action}"}

    def inspect_container(self, container: str) -> dict[str, Any]:
        encoded = urllib.parse.quote(container, safe="")
        return self._json(f"/containers/{encoded}/json", timeout=30)

    def inspect_image(self, image: str) -> dict[str, Any]:
        encoded = urllib.parse.quote(image, safe="")
        return self._json(f"/images/{encoded}/json", timeout=30)

    def create_container(self, name: str, payload: dict[str, Any]) -> str:
        body = json.dumps(payload).encode("utf-8")
        _status, _headers, raw = self._request(
            "POST", f"/containers/create?name={urllib.parse.quote(name, safe='')}",
            body=body, timeout=60,
        )
        data = json.loads(raw.decode("utf-8", errors="replace") or "{}")
        return str(data.get("Id") or "")

    def rename_container(self, container: str, new_name: str) -> None:
        encoded = urllib.parse.quote(container, safe="")
        self._request("POST", f"/containers/{encoded}/rename?name={urllib.parse.quote(new_name, safe='')}", timeout=30)

    def remove_container(self, container: str) -> None:
        encoded = urllib.parse.quote(container, safe="")
        self._request("DELETE", f"/containers/{encoded}?force=1&v=0", timeout=30)

    def set_restart_policy(self, container: str, policy: str) -> None:
        encoded = urllib.parse.quote(container, safe="")
        body = json.dumps({"RestartPolicy": {"Name": policy, "MaximumRetryCount": 0}}).encode("utf-8")
        self._request("POST", f"/containers/{encoded}/update", body=body, timeout=30)

    def restart_policies(self, containers: list[dict[str, Any]]) -> dict[str, str]:
        """按容器名返回重启策略（always/unless-stopped/no…），inspect 并发执行。"""
        policies: dict[str, str] = {}

        def fetch(row: dict[str, Any]) -> tuple[str, str]:
            name = str(row.get("Names") or row.get("Name") or "").lstrip("/")
            container_id = str(row.get("Id") or row.get("ID") or name)
            try:
                info = self.inspect_container(container_id)
                host_config = info.get("HostConfig") if isinstance(info, dict) else {}
                restart = host_config.get("RestartPolicy") if isinstance(host_config, dict) else {}
                return name, str(restart.get("Name") or "no") or "no"
            except Exception:
                return name, ""

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            for name, policy in executor.map(fetch, containers):
                if name:
                    policies[name] = policy
        return policies

    def recreate_container(self, container: str, image: str, progress: Callable[..., None] | None = None) -> dict[str, Any]:
        """拉取新镜像并按原配置原地重建容器；失败自动回滚。"""
        def report(step: str, percent: int | None = None, *, log: bool = True) -> None:
            if progress:
                try:
                    progress(step, percent, log=log)
                except TypeError:
                    progress(step, percent)
                except Exception:
                    pass

        self.assert_update_supported(self.inspect_container(container))
        report(f"拉取镜像 {image}", 5)
        log_fn = getattr(progress, "log", None)
        if progress is not None:
            layers: dict[str, list[int]] = {}
            finished_layers: set[str] = set()

            def on_pull_event(event: dict[str, Any]) -> None:
                layer = str(event.get("id") or "")
                detail = event.get("progressDetail") if isinstance(event.get("progressDetail"), dict) else {}
                current = int(detail.get("current") or 0)
                total = int(detail.get("total") or 0)
                status_text = str(event.get("status") or "")
                if layer and total > 0:
                    layers[layer] = [current, total]
                if layer and status_text in ("Pull complete", "Already exists") and layer not in finished_layers:
                    finished_layers.add(layer)
                    if callable(log_fn):
                        log_fn(f"pull: 层 {layer} {'已存在' if status_text == 'Already exists' else '下载完成'}")
                if not layer and status_text.startswith("Status:") and callable(log_fn):
                    log_fn(f"pull: {status_text}")
                if layers:
                    downloaded = sum(item[0] for item in layers.values())
                    overall = sum(item[1] for item in layers.values())
                    if overall > 0:
                        report(f"拉取镜像 {image}", 5 + int(downloaded / overall * 55), log=False)

            self.pull_image_stream(image, on_event=on_pull_event)
        else:
            pull_result = self.pull_image(image)
            if callable(log_fn):
                for line in str(pull_result.get("output") or "").splitlines():
                    if line.strip():
                        log_fn(f"pull: {line.strip()}")
        report("读取容器配置", 62)
        info = self.inspect_container(container)
        self.assert_update_supported(info)
        name = str(info.get("Name") or container).lstrip("/")
        old_id = str(info.get("Id") or container)
        was_running = bool((info.get("State") or {}).get("Running"))
        config = copy.deepcopy(info.get("Config") or {})
        config["Image"] = image
        host_config = copy.deepcopy(info.get("HostConfig") or {})
        # Inspect 的 Mounts 包含匿名卷的真实名称，复用它们避免创建空的新卷。
        explicit = {m.get("Target") for m in host_config.get("Mounts") or []}
        binds = list(host_config.get("Binds") or [])
        explicit.update(b.split(":")[1] for b in binds if ":" in b)
        for mount in info.get("Mounts") or []:
            if mount.get("Type") == "volume" and mount.get("Destination") not in explicit:
                binds.append(f"{mount['Name']}:{mount['Destination']}:{'rw' if mount.get('RW') else 'ro'}")
        host_config["Binds"] = binds
        networks = (info.get("NetworkSettings") or {}).get("Networks") or {}
        endpoints = {key: {k: copy.deepcopy(v) for k, v in value.items()
                           if k in {"IPAMConfig", "Links", "Aliases", "DriverOpts", "GwPriority"}}
                     for key, value in networks.items()}
        # Docker 自动生成的容器 ID 别名不能沿用。
        for endpoint in endpoints.values():
            if endpoint.get("Aliases"):
                endpoint["Aliases"] = [x for x in endpoint["Aliases"] if x not in {old_id, old_id[:12]}]
        payload: dict[str, Any] = {**config, "HostConfig": host_config}
        network_mode = str(host_config.get("NetworkMode") or "")
        if endpoints and network_mode not in {"host", "none"} and not network_mode.startswith("container:"):
            payload["NetworkingConfig"] = {"EndpointsConfig": endpoints}
        backup_name = f"{name}-vm-old-{uuid.uuid4().hex[:10]}"
        renamed = False
        new_id = ""
        disconnected = []
        try:
            if was_running:
                report("停止容器", 70)
                self.container_action(old_id, "stop")
            report("备份旧容器", 78)
            self.rename_container(old_id, backup_name)
            renamed = True
            # 释放旧端点的静态 IP，失败时按原配置重新连接。
            for network, endpoint in endpoints.items():
                if endpoint.get("IPAMConfig"):
                    self._request("POST", f"/networks/{urllib.parse.quote(network, safe='')}/disconnect",
                                  body=json.dumps({"Container": old_id, "Force": True}).encode())
                    disconnected.append(network)
            report("创建新容器", 86)
            new_id = self.create_container(name, payload)
            if not new_id:
                raise InfraError("Docker 未返回新容器 ID。", status=502, code="recreate_failed")
            if was_running:
                report("启动并检查新容器", 94)
                self.container_action(new_id, "start")
                self.wait_container_ready(new_id)
        except Exception as err:
            report("更新失败，回滚中", None)
            rollback_errors = []
            if new_id:
                try:
                    self.remove_container(new_id)
                except Exception as rollback_err:
                    rollback_errors.append(f"清理新容器：{rollback_err}")
            if renamed:
                try:
                    self.rename_container(old_id, name)
                except Exception as rollback_err:
                    rollback_errors.append(f"恢复名称：{rollback_err}")
            for network in disconnected:
                try:
                    self._request("POST", f"/networks/{urllib.parse.quote(network, safe='')}/connect",
                                  body=json.dumps({"Container": old_id, "EndpointConfig": endpoints[network]}).encode())
                except Exception as rollback_err:
                    rollback_errors.append(f"恢复网络 {network}：{rollback_err}")
            if was_running:
                try:
                    self.container_action(old_id, "start")
                except Exception as rollback_err:
                    rollback_errors.append(f"恢复运行：{rollback_err}")
            if rollback_errors:
                raise InfraError(f"更新失败：{err}；回滚未完成（旧容器 {old_id}）：{'；'.join(rollback_errors)}",
                                 status=502, code="rollback_failed") from err
            raise
        report("清理旧容器", 98)
        warning = ""
        try:
            self.remove_container(old_id)
        except Exception as err:
            warning = f"新容器已就绪，旧容器 {backup_name} 清理失败，请手动处理：{err}"
            report(warning, 98)
        return {"exitCode": 0, "output": f"容器 {name} 已按镜像 {image} 重建完成。", "warning": warning}

    @staticmethod
    def update_block_reason(info: dict[str, Any]) -> str:
        config = info.get("Config") or {}
        labels = config.get("Labels") or info.get("Labels") or {}
        labels = labels if isinstance(labels, dict) else {}
        identity = str(info.get("Id") or info.get("ID") or "")
        hostname = str(os.environ.get("HOSTNAME") or socket.gethostname())
        image = str(config.get("Image") or info.get("Image") or "")
        ref = _split_image_ref(image)
        own_name = str(os.environ.get("APP_INFRA_SELF_CONTAINER") or "").strip()
        name = str(info.get("Name") or info.get("Names") or "").lstrip("/")
        if (labels.get("io.vistamirror.application") == "true"
                or (ref and ref[0] == "lishiya003/vistamirror-admin")
                or (identity and len(hostname) >= 12 and identity.startswith(hostname))
                or (own_name and own_name in {identity, name})):
            return "VistaMirror 容器请在宿主机通过 Docker Compose 拉取并重建，避免更新进程停止自身。"
        host = info.get("HostConfig") or {}
        state = info.get("State") if isinstance(info.get("State"), dict) else {}
        if host.get("AutoRemove") or state.get("Paused") or info.get("State") == "paused":
            return "自动删除或暂停的容器暂不支持原地更新，请先调整状态或使用 Compose。"
        if labels.get("com.docker.swarm.service.id"):
            return "Swarm 服务请通过编排器更新。"
        return ""

    @classmethod
    def assert_update_supported(cls, info: dict[str, Any]) -> None:
        reason = cls.update_block_reason(info)
        if reason:
            raise InfraError(reason, status=409, code="container_update_protected")

    def wait_container_ready(self, container: str, *, timeout: float = 180, interval: float = 2) -> None:
        started = time.monotonic()
        while True:
            state = self.inspect_container(container).get("State") or {}
            health = (state.get("Health") or {}).get("Status")
            if not state.get("Running") or state.get("Restarting") or health == "unhealthy":
                raise InfraError("新容器未稳定运行或健康检查失败。", code="container_not_ready")
            elapsed = time.monotonic() - started
            if health == "healthy" or (not health and elapsed >= 5):
                return
            if elapsed >= timeout:
                raise InfraError("新容器健康检查超时。", code="container_health_timeout")
            time.sleep(interval)

    def pull_image(self, image: str) -> dict[str, Any]:
        return self.pull_image_stream(image)

    def pull_image_stream(self, image: str, on_event: Callable[[dict[str, Any]], None] | None = None, *, timeout: int = 1500) -> dict[str, Any]:
        """流式拉取镜像：逐行解析 Docker 的 JSON 进度事件，on_event 实时回调。"""
        if not self.available:
            raise InfraError(
                "未检测到本机 Docker Socket。请在 Compose 中挂载 /var/run/docker.sock:/var/run/docker.sock 后重建容器。",
                status=503,
                code="local_docker_socket_missing",
            )
        connection = _UnixSocketHTTPConnection(self.socket_path, timeout=max(60, min(int(timeout), 1800)))
        messages: list[str] = []
        try:
            connection.request("POST", f"/images/create?fromImage={urllib.parse.quote(image, safe='/:@')}")
            response = connection.getresponse()
            status = int(response.status)
            if status >= 400:
                detail = response.read(1024 * 1024).decode("utf-8", errors="replace").strip()
                raise InfraError(f"本机 Docker 返回 HTTP {status}{f'：{detail}' if detail else ''}", status=502, code="local_docker_api_failed")
            buffer = b""
            while True:
                try:
                    chunk = response.read1(65536)
                except (TimeoutError, socket.timeout) as err:
                    raise InfraError("拉取镜像超时：网络或镜像源异常，请检查 NAS 的 Docker 代理/加速器配置。", status=504, code="image_pull_timeout") from err
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    raw_line, buffer = buffer.split(b"\n", 1)
                    text = raw_line.decode("utf-8", errors="replace").strip()
                    if not text:
                        continue
                    try:
                        event = json.loads(text)
                    except Exception:
                        continue
                    if event.get("error"):
                        raise InfraError(f"拉取镜像失败：{event.get('error')}", status=502, code="image_pull_failed")
                    if on_event:
                        try:
                            on_event(event)
                        except Exception:
                            pass
                    status_text = str(event.get("status") or "")
                    if status_text and not event.get("progressDetail"):
                        messages.append(status_text if not event.get("id") else f"{event.get('id')}: {status_text}")
            for raw_line in buffer.split(b"\n"):
                text = raw_line.decode("utf-8", errors="replace").strip()
                if not text:
                    continue
                try:
                    event = json.loads(text)
                except Exception:
                    continue
                if event.get("error"):
                    raise InfraError(f"拉取镜像失败：{event.get('error')}", status=502, code="image_pull_failed")
                if on_event:
                    try:
                        on_event(event)
                    except Exception:
                        pass
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
        # 此进程无法继续上一次启动中的线程，不让遗留记录永久阻塞更新。
        with self._lock:
            rows = self._load()
            interrupted = False
            for row in rows:
                if row.get("status") in {"queued", "running"}:
                    row.update(status="failed", finishedAt=_now_iso(), error="服务重启导致任务中断，请检查容器和备份状态后重试。")
                    interrupted = True
            if interrupted:
                self._save(rows)

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
        completed = [row for row in rows if row.get("status") not in {"queued", "running"}][-300:]
        keep = {row.get("id") for row in completed}
        retained = [row for row in rows if row.get("status") in {"queued", "running"} or row.get("id") in keep]
        temp.write_text(json.dumps(retained, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(self.path)

    def list(self, *, limit: int = 80) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._load()
        active = [row for row in reversed(rows) if row.get("status") in {"queued", "running"}]
        finished = [row for row in reversed(rows) if row.get("status") not in {"queued", "running"}]
        return active + finished[:max(0, max(1, min(int(limit), 300)) - len(active))]

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

    def _append_log(self, operation_id: str, line: str) -> None:
        with self._lock:
            rows = self._load()
            for row in rows:
                if str(row.get("id")) == operation_id:
                    logs = row.get("logs") if isinstance(row.get("logs"), list) else []
                    logs.append(f"[{_now_iso()[11:19]}] {str(line)}")
                    row["logs"] = logs[-500:]
                    break
            self._save(rows)

    class _Reporter:
        """传给任务回调的进度上报器：report(step, percent) 更新进度并落一行日志，.log(line) 只落日志。"""

        def __init__(self, manager: "InfraOperationManager", operation_id: str) -> None:
            self._manager = manager
            self._operation_id = operation_id
            self._last_key: tuple[str, Any] = ("", None)
            self._last_at = 0.0

        def __call__(self, step: str, percent: int | float | None = None, *, log: bool = True) -> None:
            pct = None if percent is None else max(0, min(100, int(percent)))
            key = (str(step), pct)
            now = time.monotonic()
            if not log and now - self._last_at < 1.0:
                return  # 节流：同阶段同百分比的密集事件不重复写盘
            self._last_key = key
            self._last_at = now
            self._manager._update(self._operation_id, progress={
                "step": str(step),
                "percent": pct,
                "at": _now_iso(),
            })
            if log:
                self.log(str(step) if pct is None else f"{step}（{pct}%）")

        def log(self, line: str) -> None:
            self._manager._append_log(self._operation_id, line)

    def submit(
        self,
        *,
        host_id: str,
        action: str,
        target: str,
        description: str,
        callback: Callable[..., dict[str, Any]],
        with_progress: bool = False,
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
            "progress": {},
            "logs": [],
        }
        with self._lock:
            rows = self._load()
            if action == "container_update":
                for existing in rows:
                    if (existing.get("hostId") == host_id and existing.get("target") == target
                            and existing.get("action") == action and existing.get("status") in {"queued", "running"}):
                        raise InfraError("该容器已有更新任务，请等待完成。", status=409, code="update_already_pending")
            rows.append(operation)
            self._save(rows)
            target_lock = self._target_locks.setdefault(host_id, threading.Lock())

        def run() -> None:
            with target_lock:
                reporter = InfraOperationManager._Reporter(self, operation["id"])
                reporter.log(f"任务开始：{description}")
                self._update(operation["id"], status="running", startedAt=_now_iso())
                try:
                    result = callback(reporter) if with_progress else callback()
                    reporter.log("任务完成。")
                    self._update(operation["id"], status="success", finishedAt=_now_iso(), result=result, error="", progress={"step": "完成", "percent": 100, "at": _now_iso()})
                    if self.event_logger:
                        self.event_logger(level="info", module="docker", action=action, message=description, status=200, detail={"hostId": host_id, "target": target})
                except Exception as err:
                    reporter.log(f"任务失败：{str(err)[:300]}")
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
        self._update_check_cache: dict[str, dict[str, Any]] = {}
        self._inventory_details_cache: dict[str, dict[str, Any]] = {}
        self._update_check_locks: dict[str, threading.Lock] = {}
        self._update_revision: dict[str, int] = {}

    @staticmethod
    def _default_config() -> dict[str, Any]:
        return {
            "hosts": [],
            "projects": [],
            "autoUpdate": {},
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

    def _auto_update_flags(self, host_id: str) -> dict[str, bool]:
        with self._lock:
            config = self._load()
        flags = config.get("autoUpdate")
        if not isinstance(flags, dict):
            return {}
        per_host = flags.get(host_id)
        if not isinstance(per_host, dict):
            return {}
        return {str(name): bool(on) for name, on in per_host.items()}

    def _enrich_inventory(self, host_id: str, inventory: dict[str, Any]) -> dict[str, Any]:
        """给容器清单补充：服务版本、重启策略、自动更新标记。"""
        containers = inventory.get("containers") or []
        flags = self._auto_update_flags(host_id)
        key = tuple(sorted(str(row.get("ID") or row.get("Id") or row.get("Names")) for row in containers))
        with self._lock:
            cached = self._inventory_details_cache.get(host_id) or {}
        restart = dict(cached.get("restart") or {})
        version = str(cached.get("version") or "")
        if cached.get("key") != key or time.monotonic() - cached.get("at", 0) >= 60:
            try:
                if self._is_local_host_id(host_id):
                    client = self._local_docker()
                    version = str(client.version().get("Version") or "")
                    restart = client.restart_policies(containers)
                else:
                    runner = self._runner(self._host(host_id))
                    version_result = runner.run("docker version --format '{{.Server.Version}}'", timeout=20)
                    version = str(version_result.get("stdout") or "").strip()
                    policy_result = runner.run(
                        "docker ps -aq | xargs -r docker inspect --format '{{.Name}} {{.HostConfig.RestartPolicy.Name}}'",
                        timeout=30,
                    )
                    if int(policy_result.get("exitCode") or 0) == 0:
                        for line in str(policy_result.get("stdout") or "").splitlines():
                            parts = line.strip().split(None, 1)
                            if len(parts) == 2:
                                restart[parts[0].lstrip("/")] = parts[1] or "no"
            except Exception:
                pass  # 增强信息失败不影响清单主流程
            with self._lock:
                self._inventory_details_cache[host_id] = {"key": key, "at": time.monotonic(), "restart": restart, "version": version}
        for row in containers:
            name = str(row.get("Names") or row.get("Name") or "").lstrip("/")
            if name:
                row["RestartPolicy"] = restart.get(name, "")
                row["AutoUpdate"] = bool(flags.get(name))
                row["UpdateBlockedReason"] = (LocalDockerClient.update_block_reason(row) if self._is_local_host_id(host_id)
                                              else "远程容器请使用 Compose 更新。")
        inventory["serverVersion"] = version
        return inventory

    def docker_inventory(self, host_id: str) -> dict[str, Any]:
        if self._is_local_host_id(host_id):
            inventory = self._local_docker().inventory()
            return self._enrich_inventory(host_id, {"hostId": LOCAL_DOCKER_HOST_ID, **inventory, "checkedAt": _now_iso()})
        host = self._host(host_id)
        runner = self._runner(host)
        containers_result = runner.run("docker ps -a --no-trunc --format '{{json .}}'", timeout=30)
        images_result = runner.run("docker image ls --no-trunc --format '{{json .}}'", timeout=30)
        compose_result = runner.run("docker compose ls --format json", timeout=30)
        for result in (containers_result, images_result, compose_result):
            if int(result.get("exitCode") or 0) != 0:
                raise InfraError(str(result.get("stderr") or "Docker 查询失败。"), status=502, code="docker_query_failed")
        return self._enrich_inventory(host_id, {
            "hostId": host_id,
            "containers": self._json_rows(str(containers_result.get("stdout") or "")),
            "images": self._json_rows(str(images_result.get("stdout") or "")),
            "compose": self._json_rows(str(compose_result.get("stdout") or "")),
            "checkedAt": _now_iso(),
        })

    def check_image_updates(self, host_id: str, *, force: bool = False) -> dict[str, Any]:
        with self._lock:
            lock = self._update_check_locks.setdefault(host_id, threading.Lock())
        with lock:
            return self._check_image_updates(host_id, force=force)

    def _check_image_updates(self, host_id: str, *, force: bool = False) -> dict[str, Any]:
        """对比本地镜像 digest 与 Docker Hub 远端 digest，判断哪些容器有更新。

        结果按主机缓存 10 分钟，避免频繁打 registry。非 Docker Hub 镜像标记为 unsupported。
        """
        now = time.time()
        with self._lock:
            revision = self._update_revision.get(host_id, 0)
            cached = self._update_check_cache.get(host_id)
            if cached and not force and now - float(cached.get("at") or 0) < 600:
                return dict(cached["payload"])
        inventory = self.docker_inventory(host_id)
        containers = inventory.get("containers") or []
        # 按镜像去重，同一镜像只查一次远端
        images: dict[str, list[str]] = {}
        for row in containers:
            image = str(row.get("Image") or "").strip()
            name = str(row.get("Names") or row.get("Name") or "").lstrip("/")
            if image and name:
                images.setdefault(image, []).append(name)

        # 本地 digest
        container_digests: dict[str, str] = {}
        if self._is_local_host_id(host_id):
            client = self._local_docker()
            def running_digest(row: dict[str, Any]) -> tuple[str, str]:
                name = str(row.get("Names") or row.get("Name") or "").lstrip("/")
                image = str(row.get("Image") or "")
                try:
                    image_id = row.get("ImageID") or client.inspect_container(str(row.get("ID") or name)).get("Image")
                    if not image_id:
                        return name, ""
                    info = client.inspect_image(str(image_id))
                    ref = _split_image_ref(image)
                    return name, _local_image_digest(info.get("RepoDigests"), ref[0]) if ref else ""
                except Exception:
                    return name, ""
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                container_digests.update(executor.map(running_digest, containers))
        else:
            runner = self._runner(self._host(host_id))
            for row in containers:
                name = str(row.get("Names") or row.get("Name") or "").lstrip("/")
                image = str(row.get("Image") or "")
                inspected = runner.run(f"docker inspect {shlex.quote(name)} --format '{{{{.Image}}}}'", timeout=30)
                image_id = str(inspected.get("stdout") or "").strip()
                digest = ""
                if int(inspected.get("exitCode") or 0) == 0 and re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
                    result = runner.run(f"docker image inspect {shlex.quote(image_id)} --format '{{{{json .RepoDigests}}}}'", timeout=30)
                    if int(result.get("exitCode") or 0) == 0:
                        try:
                            ref = _split_image_ref(image)
                            digest = _local_image_digest(json.loads(str(result.get("stdout") or "[]")), ref[0]) if ref else ""
                        except ValueError:
                            pass
                container_digests[name] = digest

        # 远端 digest（并发，失败标记 unknown）
        def remote_digest(image: str) -> tuple[str, set[str]]:
            ref = _split_image_ref(image)
            if not ref:
                return image, set()
            try:
                return image, _docker_hub_remote_digest(ref[0], ref[1])
            except Exception:
                return image, set()

        remote_digests: dict[str, set[str]] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            for image, digest in executor.map(remote_digest, images.keys()):
                remote_digests[image] = digest

        updates: dict[str, dict[str, Any]] = {}
        by_name = {str(row.get("Names") or row.get("Name") or "").lstrip("/"): row for row in containers}
        for image, names in images.items():
            remote = remote_digests.get(image) or set()
            for name in names:
                local = container_digests.get(name) or ""
                status = ("unsupported" if not _split_image_ref(image) else
                          "unknown" if not remote or not local else
                          "current" if local in remote else "update")
                reason = str(by_name[name].get("UpdateBlockedReason") or "")
                updates[name] = {"image": image, "status": status, "updateAvailable": status == "update",
                                 "canUpdate": not reason, "blockedReason": reason,
                                 "localDigest": local[-12:], "remoteDigest": (sorted(remote)[0] if remote else "")[-12:]}
        payload = {"hostId": host_id, "updates": updates, "checkedAt": _now_iso()}
        with self._lock:
            if self._update_revision.get(host_id, 0) == revision:
                self._update_check_cache[host_id] = {"at": now, "payload": payload}
        return dict(payload)

    def submit_container_update(self, host_id: str, container: str, image: str = "") -> dict[str, Any]:
        """拉取新镜像并原地重建容器（本机）；远程主机交给 Compose 或提示手动。"""
        if not DOCKER_TARGET_PATTERN.match(str(container or "")):
            raise InfraError("容器标识格式不正确。", code="invalid_container")
        inventory = self.docker_inventory(host_id)
        row = next((item for item in inventory.get("containers") or []
                    if str(item.get("Names") or item.get("Name") or "").lstrip("/") == container), None)
        if not row:
            raise InfraError("没有找到该容器，可能已被删除。", status=404, code="container_not_found")
        target_image = str(image or row.get("Image") or "").strip()
        if not DOCKER_TARGET_PATTERN.match(target_image):
            raise InfraError("镜像名称格式不正确。", code="invalid_image")
        if self._is_local_host_id(host_id):
            client = self._local_docker()
            client.assert_update_supported(client.inspect_container(container))
            def callback(report):
                try:
                    return self._local_docker().recreate_container(container, target_image, progress=report)
                finally:
                    with self._lock:
                        self._update_revision[host_id] = self._update_revision.get(host_id, 0) + 1
                        self._update_check_cache.pop(host_id, None)
                        self._inventory_details_cache.pop(host_id, None)
        else:
            raise InfraError("远程主机上的容器请使用所属 Compose 项目的「拉取并更新」，或 SSH 到服务器手动操作。", code="remote_container_update_unsupported")
        return self.operations.submit(
            host_id=host_id,
            action="container_update",
            target=container,
            description=f"更新容器 {container} → {target_image}",
            callback=callback,
            with_progress=True,
        )

    def set_restart_policy(self, host_id: str, container: str, policy: str) -> dict[str, Any]:
        allowed = {"always", "unless-stopped", "no", "on-failure"}
        if policy not in allowed:
            raise InfraError("不支持的重启策略。", code="invalid_restart_policy")
        if not DOCKER_TARGET_PATTERN.match(str(container or "")):
            raise InfraError("容器标识格式不正确。", code="invalid_container")
        if self._is_local_host_id(host_id):
            self._local_docker().set_restart_policy(container, policy)
        else:
            self._checked_run(host_id, f"docker update --restart={shlex.quote(policy)} {shlex.quote(container)}", timeout=30)
        with self._lock:
            self._inventory_details_cache.pop(host_id, None)
        return {"hostId": host_id, "container": container, "restartPolicy": policy}

    def set_auto_update(self, host_id: str, container: str, enabled: bool) -> dict[str, Any]:
        if not DOCKER_TARGET_PATTERN.match(str(container or "")):
            raise InfraError("容器标识格式不正确。", code="invalid_container")
        if enabled:
            if not self._is_local_host_id(host_id):
                raise InfraError("远程容器请使用 Compose 更新。", code="remote_container_update_unsupported")
            client = self._local_docker()
            client.assert_update_supported(client.inspect_container(container))
        with self._lock:
            config = self._load()
            auto = config.get("autoUpdate") if isinstance(config.get("autoUpdate"), dict) else {}
            per_host = auto.get(host_id) if isinstance(auto.get(host_id), dict) else {}
            per_host[container] = bool(enabled)
            auto[host_id] = per_host
            config["autoUpdate"] = auto
            self._save(config)
        return {"hostId": host_id, "container": container, "autoUpdate": bool(enabled)}

    def run_auto_image_updates(self) -> dict[str, int]:
        """后台巡检：对开启自动更新的容器，有新版就排队重建。"""
        with self._lock:
            config = self._load()
        auto = config.get("autoUpdate") if isinstance(config.get("autoUpdate"), dict) else {}
        summary = {"checked": 0, "queued": 0}
        for host_id, per_host in auto.items():
            if not isinstance(per_host, dict):
                continue
            flagged = [name for name, on in per_host.items() if on]
            if not flagged:
                continue
            try:
                updates = self.check_image_updates(str(host_id), force=True).get("updates") or {}
            except Exception:
                continue
            for name in flagged:
                info = updates.get(name) or {}
                summary["checked"] += 1
                if info.get("updateAvailable") and info.get("canUpdate", True):
                    try:
                        self.submit_container_update(str(host_id), name)
                        summary["queued"] += 1
                    except InfraError:
                        continue
        return summary

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
