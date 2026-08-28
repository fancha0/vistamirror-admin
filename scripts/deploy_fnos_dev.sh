#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_FILE="${FNOS_DEV_CONFIG_FILE:-${PROJECT_ROOT}/.fnos-dev.env}"

if [[ -f "${CONFIG_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${CONFIG_FILE}"
  set +a
fi

FNOS_SSH_TARGET="${FNOS_SSH_TARGET:-root@192.168.5.9}"
FNOS_SSH_PORT="${FNOS_SSH_PORT:-22}"
FNOS_DEV_ROOT="${FNOS_DEV_ROOT:-/vol3/1000/docker/vistamirror-dev}"
REMOTE_SOURCE="${FNOS_DEV_ROOT%/}/source"

if [[ "${FNOS_DEV_ROOT}" != /* || "${FNOS_DEV_ROOT}" == "/" || ${#FNOS_DEV_ROOT} -lt 12 ]]; then
  echo "[ERROR] FNOS_DEV_ROOT 必须是明确且足够具体的绝对目录。"
  exit 1
fi

if [[ "${FNOS_DEV_ROOT}" == *"'"* || "${FNOS_DEV_ROOT}" == *$'\n'* ]]; then
  echo "[ERROR] FNOS_DEV_ROOT 不能包含单引号或换行。"
  exit 1
fi

if ! command -v rsync >/dev/null 2>&1; then
  echo "[ERROR] 当前 Mac 未安装 rsync。"
  exit 1
fi

echo "[1/4] 检查飞牛连接：${FNOS_SSH_TARGET}:${FNOS_SSH_PORT}"
ssh -o StrictHostKeyChecking=accept-new -p "${FNOS_SSH_PORT}" "${FNOS_SSH_TARGET}" \
  "mkdir -p '${REMOTE_SOURCE}' '${REMOTE_SOURCE}/data-dev'"

echo "[2/4] 同步构建源码到：${REMOTE_SOURCE}"
rsync -az --delete \
  --exclude '.git/' \
  --exclude '.claude/' \
  --exclude '.DS_Store' \
  --exclude '.env' \
  --exclude '.env.local' \
  --exclude '.fnos-dev.env' \
  --exclude '.venv/' \
  --exclude 'venv/' \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  --exclude '*.log' \
  --exclude 'data/' \
  --exclude 'data-dev/' \
  -e "ssh -o StrictHostKeyChecking=accept-new -p ${FNOS_SSH_PORT}" \
  "${PROJECT_ROOT}/" "${FNOS_SSH_TARGET}:${REMOTE_SOURCE}/"

if [[ -f "${CONFIG_FILE}" ]]; then
  echo "[3/4] 同步开发环境配置"
  scp -q -P "${FNOS_SSH_PORT}" "${CONFIG_FILE}" \
    "${FNOS_SSH_TARGET}:${REMOTE_SOURCE}/.env.fnos-dev"
  COMPOSE_ENV="--env-file .env.fnos-dev"
else
  echo "[3/4] 未找到 .fnos-dev.env，使用项目默认开发参数"
  COMPOSE_ENV=""
fi

echo "[4/4] 在飞牛本地构建并重启开发容器"
ssh -o StrictHostKeyChecking=accept-new -p "${FNOS_SSH_PORT}" "${FNOS_SSH_TARGET}" \
  "cd '${REMOTE_SOURCE}' && docker compose ${COMPOSE_ENV} -f docker-compose.fnos-dev.yml up -d --build --remove-orphans && docker compose ${COMPOSE_ENV} -f docker-compose.fnos-dev.yml ps"

DEV_HOST="${FNOS_SSH_TARGET#*@}"
DEV_PORT="${FNOS_DEV_ADMIN_PORT:-18091}"
echo
echo "[DONE] 开发版已部署：http://${DEV_HOST}:${DEV_PORT}"
echo "正式版 8091/8099 未被修改。"
