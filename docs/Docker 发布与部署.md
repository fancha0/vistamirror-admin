## Docker 发布与部署（可复制版）

这一节用于把当前项目做成类似 `emby-pulse` 的发布方式：代码开源 + Docker 镜像 + 一键部署。

### 1. 发布产物建议

- GitHub 仓库（源码 + 文档）
- Docker 镜像（建议同时发布 `latest` 和版本号标签，例如 `v1.0.0`）
- `docker-compose.yml`（推荐）和 `docker run`（备选）
- GitHub Release（每次发版说明）

### 2. 推荐 Dockerfile（项目根目录）

```dockerfile
FROM python:3.13-slim

WORKDIR /app
COPY . /app

EXPOSE 8080
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

CMD ["python", "dev_server.py", "--host", "0.0.0.0", "--port", "8080"]
```

### 3. 推荐 docker-compose.yml（通用服务器兼容）

```yaml
version: "3.8"

services:
  vistamirror-admin:
    image: ghcr.io/<你的GitHub用户名>/vistamirror-admin:latest
    container_name: vistamirror-admin
    restart: unless-stopped
    ports:
      - "8080:8080"
    environment:
      - TZ=Asia/Shanghai
    volumes:
      - /opt/vistamirror/invites.json:/app/invites.json
```

> 说明：
> - 如果 `8080` 端口冲突，可改成 `18080:8080`。
> - `invites.json` 用宿主机挂载，容器重启后数据不会丢。

### 4. 快速部署（服务器上直接执行）

```bash
mkdir -p /opt/vistamirror
cd /opt/vistamirror

# 将仓库代码放到当前目录（git clone 或上传）
# 然后启动：
docker compose up -d
```

访问地址：

- `http://服务器IP:8080`

### 5. 不用 compose 的单命令启动

```bash
docker run -d \
  --name vistamirror-admin \
  --restart unless-stopped \
  -p 8080:8080 \
  -v /opt/vistamirror:/app \
  -w /app \
  python:3.13-slim \
  python dev_server.py --host 0.0.0.0 --port 8080
```

### 6. 版本发布节奏（推荐）

- `main` 分支自动构建并推送 `latest`
- 打 tag（例如 `v1.0.0`）自动构建同名镜像
- 在 GitHub Release 写清：新增功能、兼容性、升级步骤、回滚方式

### 7. 上线检查清单

1. 首次打开默认进入“仪表盘”（不再出现“请选择左侧菜单”）
2. 切到其他页面后刷新，仍保留上次页面
3. `invites.json` 在容器重启后数据仍在
4. 改端口映射后仍可正常访问
5. 桌面与移动端页面都能正常加载与切换
