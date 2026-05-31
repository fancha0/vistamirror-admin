# 镜界 Vistamirror Admin（Docker 部署版）

只保留 Docker 部署说明。

## 1) 准备配置

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

## 2) 一键部署命令（复制即用）

```bash
cd /Users/sy/my-todo && cp -n .env.example .env && docker compose pull && docker compose up -d
```

## 3) docker compose 常用命令

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

## 4) 镜像版本与回滚

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

## 5) 访问地址

部署完成后访问：

- `http://<你的服务器IP或域名>:8091`
