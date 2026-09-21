#!/usr/bin/env bash
# Run on the FNOS host, in the synced source directory.
set -euo pipefail
if [[ -f .env.fnos-dev ]]; then
  set -a
  source .env.fnos-dev
  set +a
fi
# Bind metrics to Docker's host-side bridge address, not the NAS LAN address.
if [[ -z "${FNOS_NODE_EXPORTER_BIND_IP:-}" ]]; then
  FNOS_NODE_EXPORTER_BIND_IP="$(docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}')"
fi
if [[ ! "${FNOS_NODE_EXPORTER_BIND_IP}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ || "${FNOS_NODE_EXPORTER_BIND_IP}" == "0.0.0.0" ]]; then
  echo '[ERROR] 无法确定 Docker 内网地址，请在 .fnos-dev.env 设置 FNOS_NODE_EXPORTER_BIND_IP。'
  exit 1
fi
export FNOS_NODE_EXPORTER_BIND_IP

compose_dev() {
  if [[ -f .env.fnos-dev ]]; then
    docker compose --env-file .env.fnos-dev -f docker-compose.fnos-dev.yml "$@"
  else
    docker compose -f docker-compose.fnos-dev.yml "$@"
  fi
}
compose_dev up -d --build --remove-orphans
compose_dev ps
# Verify that host collectors are reachable from the application network.
echo '[CHECK] 等待宿主机指标采样…'
compose_dev exec -T vistamirror-dev python3 - <<'PY'
import os, time, urllib.request
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
# Probe the configured exporter directly; do not depend on administrator login.
url = os.environ.get('APP_NODE_EXPORTER_URL', '')
required = ('node_cpu_seconds_total', 'node_memory_MemAvailable_bytes', 'node_filesystem_size_bytes', 'node_network_receive_bytes_total')
for attempt in range(12):
    try:
        with opener.open(url, timeout=3) as response:
            body = response.read(4 * 1024 * 1024).decode()
        missing = [metric for metric in required if metric not in body]
        if missing:
            raise RuntimeError('missing: ' + ', '.join(missing))
        print('[OK] CPU、内存、文件系统和网卡计数器均已返回；首页约 5 秒后显示速率。')
        break
    except Exception as error:
        if attempt == 11:
            raise SystemExit('[ERROR] 采集器未就绪，请检查 node-exporter-dev 日志。')
        time.sleep(3)
PY
