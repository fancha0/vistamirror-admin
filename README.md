# 镜界 Vistamirror Admin

本项目支持两种运行方式：
1. 本地 Python 直接运行（开发调试）
2. Docker Compose 拉镜像运行（部署给别人使用）

## 环境变量优先（已启用）

当以下变量存在时，运行时会覆盖页面保存值：
- Emby：`APP_EMBY_SERVER_URL`、`APP_EMBY_API_KEY`、`APP_EMBY_CLIENT_NAME`
- Telegram：`APP_BOT_TELEGRAM_TOKEN`、`APP_BOT_TELEGRAM_CHAT_ID`
- 后台登录：`APP_ADMIN_*`（见下方）

页面会显示“该配置由环境变量控制”，字段只读，保存不会覆盖这类配置。未被接管字段仍可正常保存。

## 方式一：本地 Python 运行

先做“本地/ Docker 双配置分离”（避免再次出现 `/app` 只读路径问题）：

```bash
cd /Users/sy/my-todo
cp -n .env.local.example .env.local
```

编辑 `.env.local`，至少确认以下字段：
- `APP_DATA_DIR=/Users/sy/my-todo/data`
- `APP_RUNTIME_DIR=/Users/sy/my-todo/runtime`
- `APP_ADMIN_AUTH_ENABLED=1`
- `APP_ADMIN_USERNAME=<你的账号>`（可留空，改为设置页维护）
- `APP_ADMIN_PASSWORD_HASH=<PBKDF2 哈希>`（可留空，改为设置页维护）

生成管理员密码哈希（离线）：

```bash
python3 -c "from backend_modules.admin_auth_service import AdminAuthService; print(AdminAuthService.make_password_hash('把这里换成你的密码'))"
```

推荐本地启动方式（一键加载 `.env.local` + 清理旧端口进程）：

```bash
cd /Users/sy/my-todo
./scripts/run_local.sh
```

或手动启动：

```bash
cd /Users/sy/my-todo
set -a; source .env.local; set +a
PYTHONPYCACHEPREFIX=/tmp/pycache python3 dev_server.py --host 127.0.0.1 --port 8091
```

访问地址：`http://127.0.0.1:8091`

## 方式二：Docker Compose 部署（推荐）

1) 准备配置：

```bash
cd /Users/sy/my-todo
cp .env.example .env
```

2) 编辑 `.env`（至少建议填写）：
- `APP_PORT`
- `APP_EMBY_SERVER_URL`
- `APP_EMBY_API_KEY`
- `APP_BOT_TELEGRAM_TOKEN`
- `APP_BOT_TELEGRAM_CHAT_ID`
- `APP_ADMIN_USERNAME`
- `APP_ADMIN_PASSWORD_HASH`

3) 生成管理员密码哈希（离线命令）：

```bash
python3 -c "from backend_modules.admin_auth_service import AdminAuthService; print(AdminAuthService.make_password_hash('把这里换成你的密码'))"
```

把输出结果写入 `.env` 的 `APP_ADMIN_PASSWORD_HASH`。

4) 拉镜像并启动：

```bash
docker compose pull
docker compose up -d
```

5) 查看状态：

```bash
docker compose ps
docker compose logs -f
```

### 一键最短命令（复制即用）

```bash
cd /Users/sy/my-todo && cp -n .env.example .env && docker compose pull && docker compose up -d
```

## 升级与回滚

- 升级到最新：

```bash
docker compose pull
docker compose up -d
```

- 回滚到历史版本（例：`v0.1.0`）：
  - 把 `.env` 里的 `IMAGE_NAME` 改成 `fancha0/vistamirror-admin:v0.1.0`
  - 再执行：

```bash
docker compose pull
docker compose up -d
```

## 数据持久化

Compose 默认挂载：
- `./data:/app/data`
- `./runtime:/app/runtime`

容器重建后，配置与日志仍保留在宿主机目录。

## 后台登录鉴权说明

- 开启方式：`APP_ADMIN_AUTH_ENABLED=1`（默认建议开启）
- 登录会话：默认 24 小时（`APP_ADMIN_SESSION_TTL_SECONDS=86400`）
- 登录页“保持登录”：默认不勾选；勾选后会保持登录直到手动退出（适合个人设备，共享设备不建议勾选）
- 防爆破：默认 5 次失败锁定 15 分钟（`APP_ADMIN_LOGIN_MAX_FAILS=5`、`APP_ADMIN_LOGIN_LOCK_SECONDS=900`）
- 匿名开放：仅邀请码页面与注册链路（`/invite/*`、`/api/invite/{code}`、`/api/invite/{code}/register`、`/api/register`）

### 修改管理员账号密码（两种模式）

1) **环境变量接管模式（Docker 推荐）**
- 设置了 `APP_ADMIN_USERNAME` + `APP_ADMIN_PASSWORD_HASH` 时，设置页会显示“环境变量接管，只读”。
- 需要在 `.env` / `.env.local` 修改后重启服务。

2) **设置页管理模式（本地调试方便）**
- 留空 `APP_ADMIN_USERNAME` 和 `APP_ADMIN_PASSWORD_HASH`，服务会使用 `data/admin_auth.json`。
- 首次启用建议先用环境变量登录一次，再在设置页修改并生成 `data/admin_auth.json`，然后再清空环境变量切换到此模式。
- 在“系统设置 -> 后台管理员凭据”中输入当前密码、新用户名、新密码后提交。
- 修改成功会强制所有会话退出，需要使用新凭据重新登录。

## 发布者说明（GitHub + Docker Hub 自动发布）

本仓库已提供工作流：`/Users/sy/my-todo/.github/workflows/docker-publish.yml`

发布前请在 GitHub 仓库 `Settings -> Secrets and variables -> Actions` 配置：
- `DOCKERHUB_USERNAME`
- `DOCKERHUB_TOKEN`

发布规则：
- push 到 `main`：推送 `fancha0/vistamirror-admin:latest`
- push `v*` tag：推送同名版本标签（例如 `v1.0.0`）

## 你现在只需做两步（必须手动）

1. 在 GitHub 仓库 Secrets 添加：
   - `DOCKERHUB_USERNAME`
   - `DOCKERHUB_TOKEN`
2. 推一次 `main`（或打 `v*` tag）触发 Actions 自动发镜像。

## 发布前本地检查清单

1) 鉴权链路：
- 未登录 `GET /api/auth/me` 返回 `401`
- 登录后返回 `200`
- 退出后恢复 `401`

2) 匿名开放链路：
- `/invite/{code}` 页面可访问
- `/api/invite/{code}` 与注册接口可访问
- 其他后台管理接口未登录返回 `401`

3) 语法检查：

```bash
cd /Users/sy/my-todo
PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m py_compile dev_server.py backend_modules/*.py
node --check runtime/script.js
```
