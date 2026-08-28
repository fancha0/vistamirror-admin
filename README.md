# 镜界 Vistamirror Admin（Docker Compose 部署）

## 1) 默认推荐：极简部署文件

使用仓库中的极简文件：[/Users/sy/my-todo/docker-compose.simple.yml](/Users/sy/my-todo/docker-compose.simple.yml:1)

```yaml
services:
  vistamirror-admin:
    image: lishiya003/vistamirror-admin:latest
    container_name: vistamirror-admin
    restart: unless-stopped
    # 允许读取宿主机 Docker Socket；只建议用于受信任的内网管理环境。
    user: "0:0"
    ports:
      - "8091:8091"
      # STRM 已签名播放链接专用端口（/d/*）
      - "8099:8099"
    environment:
      - TZ=Asia/Shanghai
      - APP_HOST=0.0.0.0
      - APP_PORT=8091
      - APP_STRM_PLAYBACK_PORT=8099
      - APP_DATA_DIR=/app/data
      - APP_RUNTIME_DIR=/app/runtime
      - APP_ADMIN_AUTH_ENABLED=1
      - APP_ADMIN_USERNAME=admin
      - APP_ADMIN_PASSWORD=admin123
      # 用于加密保存 SSH 密码/私钥，请替换并长期保持不变。
      - APP_INFRA_MASTER_KEY=请替换为随机密钥
    volumes:
      - ./data:/app/data
      - /var/run/docker.sock:/var/run/docker.sock
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

## 5) Docker 管理

侧边栏只保留“Docker 管理”，用于集中查看本机 Compose 项目、容器、镜像、运行指标、
日志和操作活动。挂载 `/var/run/docker.sock` 后，系统会自动出现“本机 Docker”，无需额外
配置即可执行预设的启停、重启、暂停、镜像拉取和 Compose 更新操作。

Docker Socket 由 root 管理，因此 Compose 需要同时包含：

```yaml
user: "0:0"
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
```

这相当于把本机 Docker 管理权限交给 VistaMirror，只应部署在受信任的内网环境。页面只
开放预设的容器和 Compose 操作，不提供任意命令输入，也不会主动删除数据卷。

## 6) 回滚镜像版本

把 `image` 改成目标 tag（例如 `lishiya003/vistamirror-admin:v0.1.0`），然后执行：

```bash
docker compose -f docker-compose.simple.yml pull
docker compose -f docker-compose.simple.yml up -d
```

## 7) 访问地址

`http://<你的服务器IP或域名>:8091`

## 飞牛本地开发测试（无需推送 Docker Hub）

这套流程把 Mac 上的必要源码同步到飞牛，并直接在飞牛本地构建一个独立开发镜像。
正式版继续使用 `8091/8099`，开发版使用 `18091/18099`，两者的数据目录和容器名称
完全隔离。

首次使用，在 Mac 项目目录执行：

```bash
cp .fnos-dev.env.example .fnos-dev.env
```

默认值已经对应 `root@192.168.5.9` 和
`/vol3/1000/docker/vistamirror-dev`。如果飞牛 SSH 端口、媒体目录或登录信息不同，修改
`.fnos-dev.env`。随后每次测试新代码只需执行：

```bash
./scripts/deploy_fnos_dev.sh
```

脚本会依次检查 SSH、同步源码、在飞牛运行 Docker Build 并重建
`vistamirror-admin-dev`。完成后访问：

```text
http://192.168.5.9:18091
```

同步过程不会上传 `.git`、正式 `data`、本地环境文件、日志或缓存。远端开发数据保存在
`/vol3/1000/docker/vistamirror-dev/source/data-dev`，不会覆盖正式版数据。

## 8) STRM 播放端与 Emby 配置

STRM 的管理界面仍使用 `8091`；`8099` 是独立的 Emby Web/播放反代端：根路径及
`/web/*`、Emby API 会转发到已配置的 Emby，`/d/*` 则处理 VistaMirror 的签名 STRM
链接。不要把 `8091` 直接写进 STRM 的“播放外部地址”。

推荐通过独立域名或反代暴露该端口。例如反向代理的上游为 Docker 宿主机：

```nginx
location / {
  proxy_pass http://127.0.0.1:8099;
  proxy_set_header Host $host;
  proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
  proxy_set_header X-Forwarded-Proto $scheme;
  proxy_buffering off;
}
```

然后在 VistaMirror 的 STRM 页面把“STRM 播放外部地址”设置为该公网地址，例如
`https://strm.example.com`，保存配置后执行“安全增量同步”以重写 STRM 链接。

如果需要在 STRM 页面提交 Emby 刷新任务，完整版 Compose 或 `.env` 还需填写：

```text
APP_EMBY_SERVER_URL=http://172.17.0.1:8096
APP_EMBY_API_KEY=你的_Emby_API_Key
```

`APP_EMBY_SERVER_URL` 同时是 `8099` 反代的上游地址；`APP_EMBY_API_KEY` 仅用于
Emby 刷库与媒体库查询，不参与浏览器登录、115 直链解析或 STRM 播放。若 Emby在同一
Docker 网络中，优先将地址改为对应容器服务名，例如 `http://emby:8096`。

## 9) Emby Webhook 回调地址怎么固定成 VistaMirror 自己的域名

如果你是 Docker 部署，并且希望后台生成的 Emby Webhook 地址始终指向 VistaMirror 自己的公网地址，请设置：

```yaml
- VISTAMIRROR_PUBLIC_BASE_URL=https://VistaMirror.lshiya.top:333
```

或在完整版 `docker-compose.yml` / `.env` 中设置：

```text
VISTAMIRROR_PUBLIC_BASE_URL=https://VistaMirror.lshiya.top:333
```

生成出来的回调地址就会固定为：

```text
https://VistaMirror.lshiya.top:333/api/v1/webhook?token=vistamirror
```

说明：
- 这里必须填 **VistaMirror 自己的外网访问地址**，不是 Emby 地址。
- 新版本优先读取 `VISTAMIRROR_PUBLIC_BASE_URL`。
- 旧变量 `BOT_PUBLIC_BASE_URL` 仍兼容，但建议后续统一改成 `VISTAMIRROR_PUBLIC_BASE_URL`。

## 10) 影巢一键授权代理

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
