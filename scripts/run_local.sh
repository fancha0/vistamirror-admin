#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ ! -f ".env.local" ]]; then
  echo "[ERROR] Missing .env.local"
  echo "Run: cp .env.local.example .env.local"
  echo "Then fill APP_ADMIN_PASSWORD_HASH before start."
  exit 1
fi

set -a
# shellcheck disable=SC1091
source ".env.local"
set +a

HOST="${APP_HOST:-127.0.0.1}"
PORT="${APP_PORT:-8091}"

if [[ "${APP_DATA_DIR:-}" == /app/* || "${APP_RUNTIME_DIR:-}" == /app/* ]]; then
  echo "[ERROR] .env.local contains Docker-only paths (/app/*)."
  echo "Set APP_DATA_DIR and APP_RUNTIME_DIR to local writable paths."
  exit 1
fi

if [[ "${APP_ADMIN_AUTH_ENABLED:-0}" == "1" && -z "${APP_ADMIN_PASSWORD_HASH:-}" ]]; then
  echo "[ERROR] APP_ADMIN_PASSWORD_HASH is empty in .env.local"
  echo "Generate hash first, then retry:"
  echo "python3 -c \"from backend_modules.admin_auth_service import AdminAuthService; print(AdminAuthService.make_password_hash('your-password'))\""
  exit 1
fi

EXISTING_PID="$(lsof -nP -iTCP:"${PORT}" -sTCP:LISTEN -t 2>/dev/null | head -n 1 || true)"
if [[ -n "${EXISTING_PID}" ]]; then
  echo "[INFO] Port ${PORT} is occupied by PID ${EXISTING_PID}, stopping it..."
  kill "${EXISTING_PID}" || true
  sleep 1
fi

echo "[INFO] Starting local server at http://${HOST}:${PORT}"
exec env PYTHONPYCACHEPREFIX=/tmp/pycache python3 dev_server.py --host "${HOST}" --port "${PORT}"
