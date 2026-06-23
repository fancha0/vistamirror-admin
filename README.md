# 镜界 Vistamirror Admin（Docker Compose 部署）

## 1) 默认推荐：极简部署文件

使用仓库中的极简文件：[/Users/sy/my-todo/docker-compose.simple.yml](/Users/sy/my-todo/docker-compose.simple.yml:1)

```yaml
services:
  vistamirror-admin:
    image: lishiya003/vistamirror-admin:latest
    container_name: vistamirror-admin
    restart: unless-stopped
    ports:
      - "8091:8091"
    environment:
      - TZ=Asia/Shanghai
      - APP_HOST=0.0.0.0
      - APP_PORT=8091
      - APP_DATA_DIR=/app/data
      - APP_RUNTIME_DIR=/app/runtime
      - APP_ADMIN_AUTH_ENABLED=1
      - APP_ADMIN_USERNAME=admin
      - APP_ADMIN_PASSWORD=admin123
    volumes:
      - ./data:/app/data
```

启动（复制即用）：

```bash
docker compose -f docker-compose.simple.yml up -d
```

## 2) 常用运维命令（极简版）

```bash
docker compose -f docker-compose.simple.yml pull
docker compose -f docker-compose.simple.yml up -d
docker compose -f docker-compose.simple.yml ps
docker compose -f docker-compose.simple.yml logs -f
docker compose -f docker-compose.simple.yml restart
docker compose -f docker-compose.simple.yml down
```

## 3) 用户名与密码怎么设置（Compose 直接改）

直接在 `docker-compose.simple.yml` 里改这两行即可：

```yaml
- APP_ADMIN_USERNAME=admin
- APP_ADMIN_PASSWORD=你的自定义密码
```

重启生效：

```bash
docker compose -f docker-compose.simple.yml up -d --force-recreate
```

说明：
- `APP_ADMIN_PASSWORD` 是明文便捷模式，适合快速部署。
- 公网环境更推荐使用 `APP_ADMIN_PASSWORD_HASH`（哈希更安全）。
- 如果 `APP_ADMIN_PASSWORD` 和 `APP_ADMIN_PASSWORD_HASH` 同时设置，系统会优先使用 `APP_ADMIN_PASSWORD`。

## 4) 需要高级配置时

使用完整版文件：[/Users/sy/my-todo/docker-compose.yml](/Users/sy/my-todo/docker-compose.yml:1)

- 完整版里像 `${APP_PORT:-8091}` 的写法不是乱码，而是变量默认值语法。
- 含义：如果没设置 `APP_PORT`，就用默认值 `8091`。

## 5) 回滚镜像版本

把 `image` 改成目标 tag（例如 `lishiya003/vistamirror-admin:v0.1.0`），然后执行：

```bash
docker compose -f docker-compose.simple.yml pull
docker compose -f docker-compose.simple.yml up -d
```

## 6) 访问地址

`http://<你的服务器IP或域名>:8091`

## 7) 影巢一键授权代理

一键授权需要单独部署公共代理，并在影巢“我的应用”中把固定回调地址登记为：

```text
https://你的代理域名/oauth/callback
```

复制 `docker-compose.hdhive-broker.yml`，配置以下环境变量后启动：

```text
HDHIVE_BROKER_PUBLIC_URL=https://你的代理域名
HDHIVE_BROKER_CLIENT_ID=影巢应用ClientID
HDHIVE_BROKER_APP_SECRET=影巢应用Secret
HDHIVE_BROKER_ENCRYPTION_KEY=至少24字符的随机密钥
```

```bash
docker compose -f docker-compose.hdhive-broker.yml up -d
```

代理必须由 HTTPS 反向代理保护。普通 Vistamirror 实例只填写 `APP_HDHIVE_BROKER_URL`，不会取得影巢应用 Secret 或用户 Token。授权范围为 `meta query unlock write`，其中 `write` 用于普通签到。
