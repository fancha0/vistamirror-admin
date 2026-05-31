# 镜界 Vistamirror Admin（Docker 部署版）

只保留 Docker 部署说明。

## 1) 准备 `docker-compose.yml`

仓库里已包含现成文件：[/Users/sy/my-todo/docker-compose.yml](/Users/sy/my-todo/docker-compose.yml:1)。

如果你要手动新建，使用下面这份：

```yaml
services:
  vistamirror:
    image: ${IMAGE_NAME:-lishiya003/vistamirror-admin:latest}
    container_name: vistamirror-admin
    restart: unless-stopped
    env_file:
      - .env
    environment:
      APP_HOST: ${APP_HOST:-0.0.0.0}
      APP_PORT: ${APP_PORT:-8091}
      APP_DATA_DIR: /app/data
      APP_RUNTIME_DIR: /app/runtime
      APP_EMBY_SERVER_URL: ${APP_EMBY_SERVER_URL:-}
      APP_EMBY_API_KEY: ${APP_EMBY_API_KEY:-}
      APP_EMBY_CLIENT_NAME: ${APP_EMBY_CLIENT_NAME:-镜界Vistamirror User Console}
      APP_BOT_TELEGRAM_TOKEN: ${APP_BOT_TELEGRAM_TOKEN:-}
      APP_BOT_TELEGRAM_CHAT_ID: ${APP_BOT_TELEGRAM_CHAT_ID:-}
      BOT_WEBHOOK_TOKEN: ${BOT_WEBHOOK_TOKEN:-vistamirror}
      BOT_PUBLIC_BASE_URL: ${BOT_PUBLIC_BASE_URL:-}
      TG_PROXY_URL: ${TG_PROXY_URL:-}
      EMBY_PUBLIC_WEB_URL: ${EMBY_PUBLIC_WEB_URL:-}
      APP_ADMIN_AUTH_ENABLED: ${APP_ADMIN_AUTH_ENABLED:-1}
      APP_ADMIN_USERNAME: ${APP_ADMIN_USERNAME:-}
      APP_ADMIN_PASSWORD_HASH: ${APP_ADMIN_PASSWORD_HASH:-}
      APP_ADMIN_SESSION_TTL_SECONDS: ${APP_ADMIN_SESSION_TTL_SECONDS:-86400}
      APP_ADMIN_LOGIN_MAX_FAILS: ${APP_ADMIN_LOGIN_MAX_FAILS:-5}
      APP_ADMIN_LOGIN_LOCK_SECONDS: ${APP_ADMIN_LOGIN_LOCK_SECONDS:-900}
    ports:
      - "${APP_PORT:-8091}:${APP_PORT:-8091}"
    volumes:
      - ./data:/app/data
      - ./runtime:/app/runtime
    healthcheck:
      test:
        [
          "CMD-SHELL",
          "python3 -c \"import os,sys,urllib.request;port=os.getenv('APP_PORT','8091');u='http://127.0.0.1:'+port+'/';sys.exit(0 if urllib.request.urlopen(u,timeout=3).status < 500 else 1)\""
        ]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 20s
```

## 2) 准备配置

```bash
cd /Users/sy/my-todo
cp -n .env.example .env
```

编辑 `/Users/sy/my-todo/.env`，至少建议配置：

- `APP_PORT=8091`
- `APP_EMBY_SERVER_URL=...`
- `APP_EMBY_API_KEY=...`
- `APP_BOT_TELEGRAM_TOKEN=...`
- `APP_BOT_TELEGRAM_CHAT_ID=...`
- `APP_ADMIN_USERNAME=...`
- `APP_ADMIN_PASSWORD_HASH=...`

如需生成管理员密码哈希（离线）：

```bash
python3 -c "from backend_modules.admin_auth_service import AdminAuthService; print(AdminAuthService.make_password_hash('把这里换成你的密码'))"
```

把输出整串写入 `.env` 的 `APP_ADMIN_PASSWORD_HASH`。

## 3) 一键部署命令（复制即用）

```bash
cd /Users/sy/my-todo && cp -n .env.example .env && docker compose pull && docker compose up -d
```

## 4) docker compose 常用命令

启动/更新：

```bash
docker compose pull
docker compose up -d
```

查看状态：

```bash
docker compose ps
```

查看日志：

```bash
docker compose logs -f
```

重启服务：

```bash
docker compose restart
```

停止服务：

```bash
docker compose down
```

## 5) 镜像版本与回滚

默认镜像（见 `/Users/sy/my-todo/docker-compose.yml`）：

- `lishiya003/vistamirror-admin:latest`

回滚示例（切到 `v0.1.0`）：

1. 在 `.env` 中设置：
   - `IMAGE_NAME=lishiya003/vistamirror-admin:v0.1.0`
2. 执行：

```bash
docker compose pull
docker compose up -d
```

## 6) 访问地址

部署完成后访问：

- `http://<你的服务器IP或域名>:8091`
