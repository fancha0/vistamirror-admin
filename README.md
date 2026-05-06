# 镜界 Vistamirror

🎬 专为 Emby Media Server 打造的现代化综合管理面板

[![Docker Pulls](https://img.shields.io/docker/pulls/vistamirror/vistamirror?style=flat-square)](https://github.com/vistamirror/vistamirror/pkgs/container/vistamirror)
[![License](https://img.shields.io/github/license/vistamirror/vistamirror?style=flat-square)](LICENSE)
[![Last Commit](https://img.shields.io/github/last-commit/vistamirror/vistamirror?style=flat-square)](https://github.com/vistamirror/vistamirror/commits/main)

---

## 🌟 项目简介

**镜界 Vistamirror** 是一款专为 Emby Media Server 打造的现代化管理面板。它提供了精美的数据可视化、强大的用户管理系统、以及丰富的自动化通知功能。

无论是管理用户账号、监控播放数据、还是生成观影报表，镜界都能为您提供优雅的解决方案。

---

## ✨ 功能特性

### 📊 数据分析与仪表盘

- **实时状态监控**：当前并发播放数、活跃用户数、媒体库总量
- **内容排行榜**：展示热门电影和剧集
- **数据洞察**：用户观影偏好分析

### 👥 用户管理系统

- **邀请码注册**：一键生成专属注册链接，支持设置有效期（1天~永久）
- **用户生命周期管理**：支持设置账号有效期、封禁/解禁账号
- **全能编辑**：支持在后台修改用户信息、权限配置

### 🤖 Telegram 机器人

- **实时播放通知**：用户开始/暂停/恢复/停止播放时自动推送
- **入库通知**：新资源入库时发送精美通知
- **便捷指令**：`/help`、`/check`、`/sousuo`、`/ribaoday` 等

### 📨 企业微信集成

- **Webhook 回调**：支持企业微信消息推送
- **安全配置**：Token 和 AES Key 加密

### 🛠 系统特性

- **环境变量优先**：支持 Docker 环境变量注入配置
- **数据持久化**：容器重启后数据不丢失
- **多架构支持**：支持 AMD64 和 ARM64

---

## 🚀 快速部署

### 前置条件

- 服务器已安装 **Docker** 和 **Docker Compose**
- 已拥有一个运行中的 **Emby Server**
- Emby 后台生成一个 **API Key**

### 部署步骤

#### 1. 创建目录

```bash
mkdir -p /opt/vistamirror && cd /opt/vistamirror
```

#### 2. 下载 docker-compose.yml

```bash
curl -O https://raw.githubusercontent.com/vistamirror/vistamirror/main/docker-compose.yml
```

#### 3. 创建 .env 配置文件

```bash
nano .env
```

填入以下内容：

```env
# =========================
# 基础配置
# =========================
TZ=Asia/Shanghai
APP_HOST=0.0.0.0
APP_PORT=8091

# =========================
# Emby 连接配置（必须）
# =========================
APP_EMBY_SERVER_URL=http://你的emby地址:8096/emby
APP_EMBY_API_KEY=你的Emby_API_Key

# =========================
# Telegram 通知（可选）
# =========================
APP_BOT_TELEGRAM_TOKEN=
APP_BOT_TELEGRAM_CHAT_ID=
BOT_WEBHOOK_TOKEN=vistamirror

# =========================
# 公网地址（用于生成 Webhook URL）
# =========================
BOT_PUBLIC_BASE_URL=http://你的面板地址:8091
```

#### 4. 启动服务

```bash
docker compose up -d
```

#### 5. 访问面板

```
http://你的服务器IP:8091
```

---

## ⚙️ 系统配置

首次访问后，请点击侧边栏 **"系统设置"** 完成基础对接：

### 1. Emby 连接配置

- 在 Emby 后台生成一个 API Key
- 填入 "系统 API KEY" 栏
- 点击 "连接 Emby" 进行验证

### 2. Webhook 配置（启用实时通知）

1. 在镜界设置页面设置一个 Token（如 `vistamirror`）
2. 在 Emby 后台 → Webhooks → 添加 Webhook：
   - **URL**: `http://你的面板IP:8091/api/v1/webhook?token=vistamirror`
   - **事件**: 勾选 `Playback Start`, `Playback Stop`, `Playback Pause`, `Playback Resume`, `New Media Added`

### 3. Telegram 机器人（可选）

- 在 [@BotFather](https://t.me/BotFather) 创建机器人，获取 Token
- 填入 "BOT TOKEN" 和 "ADMIN CHAT ID"
- 点击 "发送测试" 验证

---

## 🔧 高级配置

### 环境变量说明

| 变量名 | 必须 | 说明 |
|--------|------|------|
| `APP_EMBY_SERVER_URL` | ✅ | Emby 服务器地址 |
| `APP_EMBY_API_KEY` | ✅ | Emby API Key |
| `APP_BOT_TELEGRAM_TOKEN` | ❌ | Telegram Bot Token |
| `APP_BOT_TELEGRAM_CHAT_ID` | ❌ | Telegram Chat ID |
| `BOT_WEBHOOK_TOKEN` | ❌ | Webhook 验证 token |
| `BOT_PUBLIC_BASE_URL` | ❌ | 公网地址（生成回调URL用） |
| `TG_PROXY_URL` | ❌ | Telegram 代理地址 |
| `EMBY_PUBLIC_WEB_URL` | ❌ | Emby 公网访问地址 |

### 数据持久化

部署后会在 `./data` 目录生成：

```
data/
├── invites.json          # 邀请码、用户配置
└── playback_events.jsonl # 播放日志
```

**删除容器不会丢失数据**，已挂载到宿主机。

---

## 📝 目录结构

```
vistamirror/
├── dev_server.py           # 主服务器入口
├── backend_modules/        # 后端核心模块
│   ├── ip_locator.py      # IP 地址解析
│   ├── message_formatter.py # 消息格式化
│   ├── notification_config.py # 通知配置
│   ├── playback_event_logger.py # 播放日志
│   ├── telegram_commands.py # Telegram 命令
│   ├── telegram_sender.py  # Telegram 发送器
│   └── webhook_receiver.py  # Webhook 处理
├── runtime/                # 前端静态资源
│   ├── index.html         # 主界面
│   ├── script.js          # 前端脚本
│   ├── styles.css         # 样式文件
│   └── register.html      # 邀请注册页
├── data/                   # 数据存储（运行时生成）
├── docker-compose.yml     # Docker 部署配置
├── Dockerfile             # Docker 镜像构建
├── requirements.txt       # Python 依赖
└── .env.example           # 环境变量模板
```

---

## 🐛 问题反馈

如果您遇到问题，请通过以下方式反馈：

- [GitHub Issues](https://github.com/vistamirror/vistamirror/issues)
- [Telegram 群组](https://t.me/vistamirror)

---

## 📄 许可证

本项目基于 [MIT License](LICENSE) 开源。

<p align="center">
  <strong>镜界 Vistamirror</strong> - 让 Emby 管理更优雅
</p>