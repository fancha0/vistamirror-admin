"""Host metrics from Node Exporter. No shell execution, Docker stats or disk writes."""
from __future__ import annotations

import copy
import json
import math
import os
import re
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable

_SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)(?:\s+\S+)?$')
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"(?:,|$)')
_VIRTUAL_FS = {'tmpfs', 'devtmpfs', 'overlay', 'squashfs', 'proc', 'sysfs', 'cgroup', 'cgroup2', 'nsfs', 'autofs', 'tracefs', 'debugfs', 'securityfs', 'fusectl', 'ramfs'}
_VIRTUAL_MOUNT = re.compile(r'^/(proc|sys|dev|run)(/|$)|/(docker/(overlay2|containers)|kubelet)(/|$)')
_VIRTUAL_NET = re.compile(r'^(lo$|docker|veth|br-|virbr|cni|flannel|tun|tap|wg)')


def parse_metrics(body: str) -> dict[str, list[tuple[dict[str, str], float]]]:
    result: dict[str, list] = {}
    for line in body.splitlines():
        if not line.startswith('node_'):
            continue
        match = _SAMPLE.match(line)
        if not match:
            continue
        try:
            value = float(match[3])
            if not math.isfinite(value):
                continue
            labels = {m[1]: json.loads('"' + m[2] + '"') for m in _LABEL.finditer(match[2] or '')}
        except (ValueError, json.JSONDecodeError):
            continue
        result.setdefault(match[1], []).append((labels, value))
    return result


def _scalar(metrics: dict, name: str) -> float | None:
    entries = metrics.get(name) or []
    return entries[0][1] if entries else None


def _disk_rows(metrics: dict) -> list[dict]:
    def mapping(name):
        return {tuple(labels.get(k, '') for k in ('device', 'fstype', 'mountpoint')): value
                for labels, value in metrics.get(name) or []}
    sizes = mapping('node_filesystem_size_bytes')
    free = mapping('node_filesystem_free_bytes')
    avail = mapping('node_filesystem_avail_bytes')
    errors = mapping('node_filesystem_device_error')
    readonly = mapping('node_filesystem_readonly')
    rows = []
    for key, total in sizes.items():
        device, fs, mount = key
        if total <= 0 or fs in _VIRTUAL_FS or _VIRTUAL_MOUNT.search(mount) or errors.get(key, 0) or key not in free:
            continue
        used = max(0, min(total, total - free[key]))
        rows.append({'device': device, 'filesystem': fs, 'mountpoint': mount, 'total': int(total),
                     'used': int(used), 'available': int(max(0, avail.get(key, free[key]))),
                     'percent': round(used / total * 100, 1), 'readOnly': bool(readonly.get(key, 0))})
    # Same device mounted through several bind/subvolume paths must not be summed twice.
    rows.sort(key=lambda r: (len(r['mountpoint']), r['mountpoint']))
    unique = {}
    for row in rows:
        unique.setdefault((row['device'], row['filesystem']), row)
    return sorted(unique.values(), key=lambda r: (not bool(re.match(r'^/vol\d+($|/)', r['mountpoint'])), r['mountpoint']))


class HostMetricsService:
    interval = 5

    def __init__(self, url: str = '', *, fetch: Callable[[], str] | None = None,
                 clock: Callable[[], float] = time.monotonic, disk: str = '', interface: str = '') -> None:
        self.url = str(url).strip()
        self.preferred_disk = disk
        self.preferred_interface = interface
        self._clock = clock
        self._fetch = fetch or self._fetch_metrics
        self._lock = threading.Lock()
        self._sample_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._previous: dict | None = None
        self._payload: dict | None = None
        self._history: list[dict] = []
        self._last_success: float | None = None
        self._error = ''
        self._disks: list[dict] = []
        self._disks_at: float | None = None
        self._filesystem_text = ''
        self._filesystem_attempt: float | None = None
        self._sampled_at: float | None = None

    @classmethod
    def from_env(cls):
        return cls(os.environ.get('APP_NODE_EXPORTER_URL', ''),
                   disk=os.environ.get('APP_HOST_METRICS_DISK', ''),
                   interface=os.environ.get('APP_HOST_METRICS_INTERFACE', ''))

    def _fetch_metrics(self) -> str:
        parsed = urllib.parse.urlsplit(self.url)
        if parsed.scheme not in {'http', 'https'} or not parsed.hostname:
            raise ValueError('invalid exporter URL')
        # Host-local metrics must not be sent through the application's global proxy.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        def scrape(collectors: list[str]) -> str:
            query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
            query = [(key, value) for key, value in query if key != 'collect[]']
            query.extend(('collect[]', collector) for collector in collectors)
            target = urllib.parse.urlunsplit(parsed._replace(query=urllib.parse.urlencode(query)))
            request = urllib.request.Request(target, headers={'Accept': 'text/plain', 'User-Agent': 'VistaMirror/1.0'})
            with opener.open(request, timeout=3) as response:
                content = response.read(4 * 1024 * 1024 + 1)
            if len(content) > 4 * 1024 * 1024:
                raise ValueError('metrics response too large')
            return content.decode('utf-8')
        fast = scrape(['cpu', 'meminfo', 'netdev', 'netclass', 'uname', 'stat'])
        now = self._clock()
        self._sampled_at = now  # Filesystem latency must not distort network rate intervals.
        if self._filesystem_attempt is None or now - self._filesystem_attempt >= 30:
            self._filesystem_attempt = now
            try:
                self._filesystem_text = scrape(['filesystem'])
            except Exception:
                self._filesystem_text = 'node_scrape_collector_success{collector="filesystem"} 0'
        return fast + '\n' + self._filesystem_text

    def start(self) -> None:
        if not self.url:
            return
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name='host-metrics', daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            started = self._clock()
            self.sample()
            self._stop.wait(max(0.1, self.interval - (self._clock() - started)))

    def sample(self) -> None:
        with self._sample_lock:
            try:
                self._sampled_at = None
                metrics = parse_metrics(self._fetch())
                cpu = {(label.get('cpu', ''), label.get('mode', '')): value
                       for label, value in metrics.get('node_cpu_seconds_total') or []
                       if label.get('mode') not in {'guest', 'guest_nice'}}
                total = _scalar(metrics, 'node_memory_MemTotal_bytes')
                available = _scalar(metrics, 'node_memory_MemAvailable_bytes')
                if not cpu or not total or available is None:
                    raise ValueError('required host collectors unavailable')
                now = self._sampled_at if self._sampled_at is not None else self._clock()
                previous = self._previous
                elapsed = now - previous['at'] if previous else 0
                comparable = previous is not None and 0 < elapsed <= 20
                percent = None
                if comparable and cpu.keys() == previous['cpu'].keys():
                    delta = {key: value - previous['cpu'][key] for key, value in cpu.items()}
                    summed = sum(delta.values())
                    if summed > 0 and all(value >= 0 for value in delta.values()):
                        idle = sum(value for (_, mode), value in delta.items() if mode == 'idle')
                        percent = round(max(0, min(100, (1 - idle / summed) * 100)), 1)
                receive = {label.get('device', ''): value for label, value in metrics.get('node_network_receive_bytes_total') or []}
                transmit = {label.get('device', ''): value for label, value in metrics.get('node_network_transmit_bytes_total') or []}
                states = {label.get('device'): label.get('operstate') for label, _ in metrics.get('node_network_info') or []}
                networks = []
                counters = {}
                for device in sorted(receive.keys() & transmit.keys()):
                    if _VIRTUAL_NET.match(device):
                        continue
                    counters[device] = (receive[device], transmit[device])
                    down = up = None
                    old = previous['network'].get(device) if comparable else None
                    if old and receive[device] >= old[0] and transmit[device] >= old[1]:
                        down = round((receive[device] - old[0]) / elapsed, 1)
                        up = round((transmit[device] - old[1]) / elapsed, 1)
                    networks.append({'name': device, 'receivePerSecond': down, 'transmitPerSecond': up,
                                     'state': states.get(device, 'unknown')})
                networks.sort(key=lambda r: (r['state'] not in {'up', 'unknown'}, not r['name'].startswith(('bond', 'team')), r['name']))
                stamp = datetime.now(timezone.utc).isoformat()
                boot = _scalar(metrics, 'node_boot_time_seconds')
                uname = metrics.get('node_uname_info') or [({}, 0)]
                disk_updated = self._disks_at is None or now - self._disks_at >= 30
                # Disk collector failures retain old disks with an explicit stale flag.
                disk_ok = bool(metrics.get('node_filesystem_size_bytes')) and not any(value for _, value in metrics.get('node_filesystem_device_error') or []) and all(
                    value == 1 for labels, value in metrics.get('node_scrape_collector_success') or [] if labels.get('collector') == 'filesystem')
                if disk_updated and disk_ok:
                    self._disks = _disk_rows(metrics)
                    self._disks_at = now
                sample = {'at': now, 'checkedAt': stamp, 'cpuPercent': percent,
                          'network': {r['name']: {'receivePerSecond': r['receivePerSecond'], 'transmitPerSecond': r['transmitPerSecond']} for r in networks}}
                self._history = [row for row in self._history if now - row['at'] <= 60]
                self._history.append(sample)
                used = max(0, min(total, total - available))
                payload = {'source': 'node-exporter', 'hostname': uname[0][0].get('nodename', '宿主机'),
                           'cpuCount': len({core for core, _ in cpu}), 'cpuPercent': percent,
                           'memory': {'total': int(total), 'used': int(used), 'available': int(available), 'percent': round(used / total * 100, 1)},
                           'disks': copy.deepcopy(self._disks), 'disksStale': not disk_ok,
                           'interfaces': networks, 'preferredDisk': self.preferred_disk, 'preferredInterface': self.preferred_interface,
                           'uptimeSeconds': max(0, int(time.time() - boot)) if boot is not None else None,
                           'checkedAt': stamp, 'history': [{k: v for k, v in row.items() if k != 'at'} for row in self._history]}
                self._previous = {'at': now, 'cpu': cpu, 'network': counters}
                with self._lock:
                    self._payload = payload
                    self._last_success = now
                    self._error = ''
            except Exception:
                with self._lock:
                    self._error = '无法连接采集器或指标不完整，请检查开发版采集容器状态。'

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            payload = copy.deepcopy(self._payload or {})
            age = self._clock() - self._last_success if self._last_success is not None else None
            state = ('disabled' if not self.url else 'unavailable' if not payload and self._error else
                     'starting' if not payload else 'stale' if self._error or age is None or age > 15 else
                     'warming' if payload.get('cpuPercent') is None else 'ready')
            payload.update(status=state, online=state in {'ready', 'warming'}, error=self._error,
                           ageSeconds=round(age, 1) if age is not None else None, intervalSeconds=self.interval)
            return payload
