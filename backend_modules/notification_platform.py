from __future__ import annotations

import copy
import json
import re
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .notification_config import default_library_templates, normalize_bot_config
from .telegram_sender import TelegramSender

CHANNEL_KEYS = ("telegram", "wecom")
REAL_EVENT_KEYS = (
    "playback.start",
    "playback.pause",
    "playback.resume",
    "playback.stop",
    "library.single",
    "library.grouped",
)
DEFAULT_EVENT_DISPLAY = {
    "playback.start": {"label": "开始播放", "description": "用户开始播放媒体时发送通知。"},
    "playback.pause": {"label": "暂停播放", "description": "用户暂停播放时发送通知。"},
    "playback.resume": {"label": "恢复播放", "description": "用户恢复播放时发送通知。"},
    "playback.stop": {"label": "停止播放", "description": "用户停止播放时发送通知。"},
    "library.single": {"label": "入库通知", "description": "单个电影或单集入库时发送通知。"},
    "library.grouped": {"label": "剧集聚合入库", "description": "同一剧集短时间多集入库时合并发送。"},
}
FUTURE_EVENT_KEYS = (
    "organize.completed",
    "wash.completed",
    "transfer.completed",
    "task.completed",
    "checkin.completed",
)
TEMPLATE_VAR_PATTERN = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")
TELEGRAM_CHAT_ID_PATTERN = re.compile(r"^-?\d+$")


def _default_playback_template() -> str:
    return (
        "{{title_line}}\n\n"
        "{{user_line}}\n"
        "{{playback_method_line}}\n"
        "{{media_spec_line}}\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📋 播放数据\n"
        "{{rating_line}}\n"
        "{{progress_line}}\n"
        "\n"
        "🛋️ 终端状态\n"
        "{{device_line}}\n"
        "{{ip_line}}\n"
        "{{time_line}}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "{{overview_line}}"
    )


def _legacy_playback_templates() -> tuple[str, ...]:
    return (
        "{{headline}}\n\n{{meta_line}}\n{{progress_line}}\n{{ip_line}}\n{{device_line}}\n🕒 时间：{{occurred_at}}\n{{overview_block}}",
        "{{title_line}}\n\n{{rating_line}}\n{{content_type_line}}\n{{progress_line}}\n{{ip_line}}\n{{device_line}}\n{{time_line}}\n\n{{overview_line}}",
    )


def _maybe_upgrade_playback_template(key: str, value: str, default_value: str) -> str:
    if key not in {"playback.start", "playback.pause", "playback.resume", "playback.stop"}:
        return value
    normalized = str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if normalized in {template.strip() for template in _legacy_playback_templates()}:
        return default_value
    return value


def default_notification_event_templates() -> dict[str, dict[str, str]]:
    library_templates = default_library_templates()
    playback_template = _default_playback_template()
    telegram = {
        "playback.start": playback_template,
        "playback.pause": playback_template,
        "playback.resume": playback_template,
        "playback.stop": playback_template,
        "library.single": str(library_templates.get("single") or ""),
        "library.grouped": str(library_templates.get("grouped") or ""),
    }
    wecom = dict(telegram)
    return {
        "telegram": telegram,
        "wecom": wecom,
    }


def default_notification_event_display() -> dict[str, dict[str, dict[str, str]]]:
    return {
        channel: {
            key: {
                "label": str(meta.get("label") or key),
                "description": str(meta.get("description") or ""),
            }
            for key, meta in DEFAULT_EVENT_DISPLAY.items()
        }
        for channel in CHANNEL_KEYS
    }


def default_notification_config() -> dict[str, Any]:
    templates = default_notification_event_templates()
    return {
        "enabled": True,
        "channels": {
            "telegram": {
                "enabled": True,
                "botToken": "",
                "chatId": "",
                "enableCommands": True,
                "proxyUrl": "",
            },
            "wecom": {
                "enabled": False,
                "corpId": "",
                "agentId": "",
                "secret": "",
                "toUser": "@all",
                "callbackToken": "",
                "callbackAes": "",
                "callbackUrl": "",
                "proxyUrl": "",
            },
        },
        "routes": {
            "telegram": {
                "playback.start": True,
                "playback.pause": True,
                "playback.resume": True,
                "playback.stop": True,
                "library.single": True,
                "library.grouped": True,
            },
            "wecom": {
                "playback.start": False,
                "playback.pause": False,
                "playback.resume": False,
                "playback.stop": False,
                "library.single": False,
                "library.grouped": False,
            },
        },
        "templates": templates,
        "display": default_notification_event_display(),
        "runtime": {
            "dedupeSeconds": 10,
            "playback": {
                "showIp": True,
                "showIpGeo": True,
                "showOverview": True,
                "userScope": {
                    "mode": "all",
                    "selectedUserNames": [],
                    "selectedUsersMeta": [],
                },
            },
        },
    }


def migrate_bot_config_to_notification_config(bot_config: Any) -> dict[str, Any]:
    bot = normalize_bot_config(bot_config)
    defaults = default_notification_config()
    library_templates = bot.get("libraryTemplates") if isinstance(bot.get("libraryTemplates"), dict) else {}
    telegram_playback_enabled = bool(bot.get("enablePlayback", True))
    telegram_library_enabled = bool(bot.get("enableLibrary", True))
    telegram_routes = {
        "playback.start": telegram_playback_enabled and bool(bot.get("notifyEvents", {}).get("start", True)),
        "playback.pause": telegram_playback_enabled and bool(bot.get("notifyEvents", {}).get("pause", True)),
        "playback.resume": telegram_playback_enabled and bool(bot.get("notifyEvents", {}).get("resume", True)),
        "playback.stop": telegram_playback_enabled and bool(bot.get("notifyEvents", {}).get("stop", True)),
        "library.single": telegram_library_enabled,
        "library.grouped": telegram_library_enabled,
    }
    return {
        "enabled": bool(bot.get("enableCore", True)),
        "channels": {
            "telegram": {
                "enabled": bool(bot.get("enableCore", True)),
                "botToken": str(bot.get("telegramToken") or "").strip(),
                "chatId": str(bot.get("telegramChatId") or "").strip(),
                "enableCommands": bool(bot.get("enableCommands", True)),
                "proxyUrl": "",
            },
            "wecom": {
                "enabled": False,
                "corpId": str(bot.get("wechatCorpId") or "").strip(),
                "agentId": str(bot.get("wechatAgentId") or "").strip(),
                "secret": str(bot.get("wechatSecret") or "").strip(),
                "toUser": str(bot.get("wechatToUser") or "@all").strip() or "@all",
                "callbackToken": str(bot.get("wechatCallbackToken") or "").strip(),
                "callbackAes": str(bot.get("wechatCallbackAes") or "").strip(),
                "callbackUrl": "",
                "proxyUrl": "",
            },
        },
        "routes": {
            "telegram": telegram_routes,
            "wecom": dict(defaults["routes"]["wecom"]),
        },
        "templates": {
            "telegram": {
                **defaults["templates"]["telegram"],
                "library.single": str(library_templates.get("single") or defaults["templates"]["telegram"]["library.single"]),
                "library.grouped": str(library_templates.get("grouped") or defaults["templates"]["telegram"]["library.grouped"]),
            },
            "wecom": {
                **defaults["templates"]["wecom"],
                "library.single": str(library_templates.get("single") or defaults["templates"]["wecom"]["library.single"]),
                "library.grouped": str(library_templates.get("grouped") or defaults["templates"]["wecom"]["library.grouped"]),
            },
        },
        "runtime": {
            "dedupeSeconds": int(bot.get("eventDedupSeconds") or defaults["runtime"]["dedupeSeconds"]),
            "playback": {
                "showIp": bool(bot.get("showIp", True)),
                "showIpGeo": bool(bot.get("showIpGeo", True)),
                "showOverview": bool(bot.get("showOverview", True)),
                "userScope": {
                    "mode": "all",
                    "selectedUserNames": [],
                    "selectedUsersMeta": [],
                },
            },
        },
    }


def sync_notification_config_to_bot_config(notification_config: Any, current_bot: Any = None) -> dict[str, Any]:
    config = normalize_notification_config(notification_config)
    current = normalize_bot_config(current_bot or {})
    telegram = config["channels"]["telegram"]
    wecom = config["channels"]["wecom"]
    telegram_routes = config["routes"]["telegram"]
    runtime = config["runtime"]
    playback_runtime = runtime["playback"]
    library_templates = config["templates"]["telegram"]
    playback_routes_any = any(
        bool(config["routes"][channel].get(key, False))
        for channel in CHANNEL_KEYS
        for key in ("playback.start", "playback.pause", "playback.resume", "playback.stop")
    )
    library_routes_any = any(
        bool(config["routes"][channel].get(key, False))
        for channel in CHANNEL_KEYS
        for key in ("library.single", "library.grouped")
    )
    merged = dict(current)
    merged.update(
        {
            "enableCore": bool(config.get("enabled", True)),
            "enablePlayback": playback_routes_any,
            "enableLibrary": library_routes_any,
            "telegramToken": str(telegram.get("botToken") or "").strip(),
            "telegramChatId": str(telegram.get("chatId") or "").strip(),
            "wechatCorpId": str(wecom.get("corpId") or "").strip(),
            "wechatAgentId": str(wecom.get("agentId") or "").strip(),
            "wechatSecret": str(wecom.get("secret") or "").strip(),
            "wechatToUser": str(wecom.get("toUser") or "@all").strip() or "@all",
            "wechatCallbackToken": str(wecom.get("callbackToken") or "").strip(),
            "wechatCallbackAes": str(wecom.get("callbackAes") or "").strip(),
            "enableCommands": bool(telegram.get("enableCommands", True)),
            "notifyEvents": {
                "start": bool(telegram_routes.get("playback.start", True)),
                "pause": bool(telegram_routes.get("playback.pause", True)),
                "resume": bool(telegram_routes.get("playback.resume", True)),
                "stop": bool(telegram_routes.get("playback.stop", True)),
            },
            "showIp": bool(playback_runtime.get("showIp", True)),
            "showIpGeo": bool(playback_runtime.get("showIpGeo", True)),
            "showOverview": bool(playback_runtime.get("showOverview", True)),
            "eventDedupSeconds": int(runtime.get("dedupeSeconds") or 10),
            "libraryTemplates": {
                "single": str(library_templates.get("library.single") or ""),
                "grouped": str(library_templates.get("library.grouped") or ""),
            },
        }
    )
    return normalize_bot_config(merged)


def _normalize_route_map(raw: Any, defaults: dict[str, bool]) -> dict[str, bool]:
    source = raw if isinstance(raw, dict) else {}
    return {key: bool(source.get(key, defaults.get(key, False))) for key in REAL_EVENT_KEYS}


def _normalize_template_map(raw: Any, defaults: dict[str, str]) -> dict[str, str]:
    source = raw if isinstance(raw, dict) else {}
    result: dict[str, str] = {}
    for key in REAL_EVENT_KEYS:
        value = source.get(key, defaults.get(key, ""))
        if not isinstance(value, str):
            value = defaults.get(key, "")
        value = value.replace("\r\n", "\n").replace("\r", "\n")
        result[key] = _maybe_upgrade_playback_template(key, value, str(defaults.get(key, "")))
    return result


def _normalize_display_map(raw: Any, defaults: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    source = raw if isinstance(raw, dict) else {}
    result: dict[str, dict[str, str]] = {}
    for key in REAL_EVENT_KEYS:
        source_item = source.get(key) if isinstance(source.get(key), dict) else {}
        default_item = defaults.get(key) if isinstance(defaults.get(key), dict) else {}
        label = str(source_item.get("label", default_item.get("label", key)) or "").strip() or str(default_item.get("label", key))
        description = str(source_item.get("description", default_item.get("description", "")) or "").strip()
        result[key] = {
            "label": label,
            "description": description,
        }
    return result


def _normalize_playback_user_scope(raw: Any, defaults: dict[str, Any]) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    default_mode = str(defaults.get("mode") or "all").strip().lower() or "all"
    mode = str(source.get("mode", default_mode) or default_mode).strip().lower() or default_mode
    if mode not in {"all", "selected"}:
        mode = default_mode if default_mode in {"all", "selected"} else "all"

    selected_user_names: list[str] = []
    selected_name_keys: set[str] = set()
    raw_names = source.get("selectedUserNames") if isinstance(source.get("selectedUserNames"), list) else defaults.get("selectedUserNames", [])
    for value in raw_names if isinstance(raw_names, list) else []:
        safe_value = str(value or "").strip()
        safe_key = safe_value.casefold()
        if not safe_value or safe_key in selected_name_keys:
            continue
        selected_name_keys.add(safe_key)
        selected_user_names.append(safe_value)

    selected_users_meta: list[dict[str, str]] = []
    selected_meta_keys: set[tuple[str, str]] = set()
    raw_meta = source.get("selectedUsersMeta") if isinstance(source.get("selectedUsersMeta"), list) else defaults.get("selectedUsersMeta", [])
    for row in raw_meta if isinstance(raw_meta, list) else []:
        if not isinstance(row, dict):
            continue
        user_name = str(row.get("name") or row.get("userName") or "").strip()
        user_id = str(row.get("id") or row.get("userId") or "").strip()
        if not user_name and not user_id:
            continue
        dedupe_key = (user_name.casefold(), user_id.casefold())
        if dedupe_key in selected_meta_keys:
            continue
        selected_meta_keys.add(dedupe_key)
        if user_name and user_name.casefold() not in selected_name_keys:
            selected_name_keys.add(user_name.casefold())
            selected_user_names.append(user_name)
        selected_users_meta.append(
            {
                "id": user_id,
                "name": user_name,
            }
        )

    return {
        "mode": mode,
        "selectedUserNames": selected_user_names,
        "selectedUsersMeta": selected_users_meta,
    }


def normalize_notification_config(raw: Any, legacy_bot_config: Any = None) -> dict[str, Any]:
    defaults = default_notification_config()
    legacy = migrate_bot_config_to_notification_config(legacy_bot_config or {})
    source = raw if isinstance(raw, dict) else {}

    enabled = bool(source.get("enabled", legacy.get("enabled", defaults["enabled"])))

    raw_channels = source.get("channels") if isinstance(source.get("channels"), dict) else {}
    legacy_channels = legacy.get("channels") if isinstance(legacy.get("channels"), dict) else {}
    telegram_source = raw_channels.get("telegram") if isinstance(raw_channels.get("telegram"), dict) else {}
    telegram_legacy = legacy_channels.get("telegram") if isinstance(legacy_channels.get("telegram"), dict) else {}
    wecom_source = raw_channels.get("wecom") if isinstance(raw_channels.get("wecom"), dict) else {}
    wecom_legacy = legacy_channels.get("wecom") if isinstance(legacy_channels.get("wecom"), dict) else {}

    raw_routes = source.get("routes") if isinstance(source.get("routes"), dict) else {}
    legacy_routes = legacy.get("routes") if isinstance(legacy.get("routes"), dict) else {}
    raw_templates = source.get("templates") if isinstance(source.get("templates"), dict) else {}
    legacy_templates = legacy.get("templates") if isinstance(legacy.get("templates"), dict) else {}
    raw_display = source.get("display") if isinstance(source.get("display"), dict) else {}

    raw_runtime = source.get("runtime") if isinstance(source.get("runtime"), dict) else {}
    legacy_runtime = legacy.get("runtime") if isinstance(legacy.get("runtime"), dict) else {}
    raw_playback_runtime = raw_runtime.get("playback") if isinstance(raw_runtime.get("playback"), dict) else {}
    legacy_playback_runtime = legacy_runtime.get("playback") if isinstance(legacy_runtime.get("playback"), dict) else {}

    try:
        dedupe_seconds = int(raw_runtime.get("dedupeSeconds", legacy_runtime.get("dedupeSeconds", defaults["runtime"]["dedupeSeconds"])))
    except (TypeError, ValueError):
        dedupe_seconds = int(defaults["runtime"]["dedupeSeconds"])
    dedupe_seconds = max(1, min(120, dedupe_seconds))

    telegram_defaults = defaults["channels"]["telegram"]
    wecom_defaults = defaults["channels"]["wecom"]
    templates_default = defaults["templates"]
    display_default = defaults["display"]
    playback_scope_default = defaults["runtime"]["playback"].get(
        "userScope",
        {"mode": "all", "selectedUserNames": [], "selectedUsersMeta": []},
    )
    legacy_playback_scope = (
        legacy_playback_runtime.get("userScope")
        if isinstance(legacy_playback_runtime.get("userScope"), dict)
        else playback_scope_default
    )
    raw_playback_scope = raw_playback_runtime.get("userScope") if isinstance(raw_playback_runtime.get("userScope"), dict) else legacy_playback_scope

    return {
        "enabled": enabled,
        "channels": {
            "telegram": {
                "enabled": bool(telegram_source.get("enabled", telegram_legacy.get("enabled", telegram_defaults["enabled"]))),
                "botToken": str(telegram_source.get("botToken", telegram_legacy.get("botToken", telegram_defaults["botToken"])) or "").strip(),
                "chatId": str(telegram_source.get("chatId", telegram_legacy.get("chatId", telegram_defaults["chatId"])) or "").strip(),
                "enableCommands": bool(
                    telegram_source.get("enableCommands", telegram_legacy.get("enableCommands", telegram_defaults["enableCommands"]))
                ),
                "proxyUrl": str(telegram_source.get("proxyUrl", telegram_legacy.get("proxyUrl", telegram_defaults["proxyUrl"])) or "").strip(),
            },
            "wecom": {
                "enabled": bool(wecom_source.get("enabled", wecom_legacy.get("enabled", wecom_defaults["enabled"]))),
                "corpId": str(wecom_source.get("corpId", wecom_legacy.get("corpId", wecom_defaults["corpId"])) or "").strip(),
                "agentId": str(wecom_source.get("agentId", wecom_legacy.get("agentId", wecom_defaults["agentId"])) or "").strip(),
                "secret": str(wecom_source.get("secret", wecom_legacy.get("secret", wecom_defaults["secret"])) or "").strip(),
                "toUser": str(wecom_source.get("toUser", wecom_legacy.get("toUser", wecom_defaults["toUser"])) or "").strip()
                or wecom_defaults["toUser"],
                "callbackToken": str(
                    wecom_source.get("callbackToken", wecom_legacy.get("callbackToken", wecom_defaults["callbackToken"])) or ""
                ).strip(),
                "callbackAes": str(wecom_source.get("callbackAes", wecom_legacy.get("callbackAes", wecom_defaults["callbackAes"])) or "").strip(),
                "callbackUrl": str(wecom_source.get("callbackUrl", wecom_legacy.get("callbackUrl", wecom_defaults["callbackUrl"])) or "").strip(),
                "proxyUrl": str(wecom_source.get("proxyUrl", wecom_legacy.get("proxyUrl", wecom_defaults["proxyUrl"])) or "").strip(),
            },
        },
        "routes": {
            "telegram": _normalize_route_map(raw_routes.get("telegram"), legacy_routes.get("telegram") if isinstance(legacy_routes.get("telegram"), dict) else defaults["routes"]["telegram"]),
            "wecom": _normalize_route_map(raw_routes.get("wecom"), legacy_routes.get("wecom") if isinstance(legacy_routes.get("wecom"), dict) else defaults["routes"]["wecom"]),
        },
        "templates": {
            "telegram": _normalize_template_map(raw_templates.get("telegram"), legacy_templates.get("telegram") if isinstance(legacy_templates.get("telegram"), dict) else templates_default["telegram"]),
            "wecom": _normalize_template_map(raw_templates.get("wecom"), legacy_templates.get("wecom") if isinstance(legacy_templates.get("wecom"), dict) else templates_default["wecom"]),
        },
        "display": {
            "telegram": _normalize_display_map(raw_display.get("telegram"), display_default["telegram"]),
            "wecom": _normalize_display_map(raw_display.get("wecom"), display_default["wecom"]),
        },
        "runtime": {
            "dedupeSeconds": dedupe_seconds,
            "playback": {
                "showIp": bool(raw_playback_runtime.get("showIp", legacy_playback_runtime.get("showIp", defaults["runtime"]["playback"]["showIp"]))),
                "showIpGeo": bool(
                    raw_playback_runtime.get(
                        "showIpGeo",
                        legacy_playback_runtime.get("showIpGeo", defaults["runtime"]["playback"]["showIpGeo"]),
                    )
                ),
                "showOverview": bool(
                    raw_playback_runtime.get(
                        "showOverview",
                        legacy_playback_runtime.get("showOverview", defaults["runtime"]["playback"]["showOverview"]),
                    )
                ),
                "userScope": _normalize_playback_user_scope(raw_playback_scope, playback_scope_default),
            },
        },
    }


def validate_notification_config(raw: Any, legacy_bot_config: Any = None) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(raw, dict):
        return None, "notificationConfig 必须是对象"
    config = normalize_notification_config(raw, legacy_bot_config=legacy_bot_config)

    telegram = config["channels"]["telegram"]
    playback_scope = config["runtime"]["playback"].get("userScope", {})
    token = str(telegram.get("botToken") or "").strip()
    chat_id = str(telegram.get("chatId") or "").strip()
    if (token and not chat_id) or (chat_id and not token):
        return None, "Telegram Bot Token 与 Chat ID 需要同时填写或同时留空"
    if token and ":" not in token:
        return None, "Telegram Bot Token 格式看起来不正确"
    if chat_id and not TELEGRAM_CHAT_ID_PATTERN.match(chat_id):
        return None, "Telegram Chat ID 格式不正确，应为数字"
    if str(playback_scope.get("mode") or "all").strip() == "selected" and not playback_scope.get("selectedUserNames"):
        return None, "播放通知选择了“只通知指定用户”后，至少需要勾选 1 个 Emby 用户"

    for channel in CHANNEL_KEYS:
        templates = config["templates"][channel]
        for key in REAL_EVENT_KEYS:
            value = str(templates.get(key) or "")
            if not value.strip():
                return None, f"{channel} 的模板 {key} 不能为空"
            if len(value) > 4000:
                return None, f"{channel} 的模板 {key} 过长，请控制在 4000 字以内"
        display = config["display"][channel]
        for key in REAL_EVENT_KEYS:
            label = str(display.get(key, {}).get("label") or "").strip()
            description = str(display.get(key, {}).get("description") or "").strip()
            if not label:
                return None, f"{channel} 的显示标题 {key} 不能为空"
            if len(label) > 40:
                return None, f"{channel} 的显示标题 {key} 过长，请控制在 40 字以内"
            if len(description) > 120:
                return None, f"{channel} 的说明文案 {key} 过长，请控制在 120 字以内"

    return config, None


def deepcopy_default_notification_config() -> dict[str, Any]:
    return copy.deepcopy(default_notification_config())


def extract_template_variables(template: str) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for match in TEMPLATE_VAR_PATTERN.finditer(str(template or "")):
        key = str(match.group(1) or "").strip()
        if key and key not in seen:
            seen.add(key)
            result.append(key)
    return result


def render_notification_template(template: str, payload: dict[str, Any], *, collapse_blank_lines: bool = True) -> str:
    safe_payload = payload if isinstance(payload, dict) else {}
    rendered = TEMPLATE_VAR_PATTERN.sub(lambda match: str(safe_payload.get(str(match.group(1) or "").strip(), "")), str(template or ""))
    if not collapse_blank_lines:
        return rendered
    lines = [line.rstrip() for line in rendered.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    compact: list[str] = []
    blank_seen = False
    for line in lines:
        if line.strip():
            compact.append(line)
            blank_seen = False
            continue
        if compact and not blank_seen:
            compact.append("")
            blank_seen = True
    while compact and not compact[-1].strip():
        compact.pop()
    return "\n".join(compact)


def notification_capabilities() -> dict[str, Any]:
    defaults = default_notification_event_templates()
    playback_variables = [
        {"key": "title_line", "label": "顶部标题", "description": "通知最上面的大标题，会按播放状态显示绿点、黄点、红点，并带上电影名或“剧名 - S1, Ep134 - 单集名”这种标题。"},
        {"key": "user_line", "label": "播放用户", "description": "这一行会直接显示是谁在播放，例如“🍿 播放用户：Admin”。"},
        {"key": "playback_method_line", "label": "播放策略", "description": "这一行会显示直接播放、直接串流或转码播放；如果拿不到策略，会自动留空。"},
        {"key": "media_spec_line", "label": "媒体规格", "description": "这一行会显示分辨率、视频编码、音频编码，例如“2160p | H.265 | AAC”。"},
        {"key": "rating_line", "label": "评分这一行", "description": "会显示“▸ 评分：🌟 6.1 / 10”这种格式，没有评分时会自动显示“暂无”。"},
        {"key": "content_type_line", "label": "类型这一行", "description": "兼容变量，仍可单独显示电影、剧集等类型。"},
        {"key": "progress_line", "label": "播放进度", "description": "会显示当前进度、总时长和百分比，并跟随状态显示绿点、黄点、红点。"},
        {"key": "ip_line", "label": "网络这一行", "description": "会显示公网 IP 和归属地，例如“▸ 网络：📍 36.248.233.223 福建 厦门”；关闭 IP 显示后会自动隐藏。"},
        {"key": "device_line", "label": "设备这一行", "description": "会同时显示播放器客户端和终端设备，例如“▸ 设备：📺 SenPlayer iPhone11”。"},
        {"key": "time_line", "label": "时间这一行", "description": "会显示“▸ 时间：⏰ 2026-07-06 19:24:13”这种格式。"},
        {"key": "overview_line", "label": "简介这一段", "description": "会显示“📖 剧情简介：...”这一段；剧集会优先显示当前这一集的简介，关闭简介显示后会自动隐藏。"},
        {"key": "headline", "label": "兼容旧标题", "description": "旧模板兼容变量，作用等同于整条标题。"},
        {"key": "meta_line", "label": "兼容旧评分类型", "description": "旧模板兼容变量，会把评分和类型合并成一行。"},
        {"key": "occurred_at", "label": "纯时间值", "description": "只代表时间本身，不带“时间：”前缀。"},
        {"key": "overview_block", "label": "兼容旧剧情块", "description": "旧模板兼容变量，会输出完整的剧情简介这一段。"},
        {"key": "user_name", "label": "用户名", "description": "只代表触发播放的人名。"},
        {"key": "title", "label": "当前标题", "description": "只代表当前播放这一集或这一部的标题。"},
        {"key": "action_text", "label": "动作文字", "description": "只代表“开始播放、暂停播放、恢复播放、停止播放”。"},
        {"key": "series_name", "label": "剧名", "description": "只代表剧集所属的系列名称。"},
        {"key": "episode_tag", "label": "集号", "description": "只代表像 S01E01 这样的集号。"},
    ]
    library_variables = [
        {"key": "entry_label", "label": "通知类型标题", "description": "会显示单集入库、电影入库、整季入库这种标题文字，适合放在最上面。"},
        {"key": "summary_line", "label": "评分摘要这一行", "description": "会自动整理成“评分 + 状态”或“评分 + 影片时长”这一行。"},
        {"key": "tagline_line", "label": "短标语这一行", "description": "会显示一句话简介这一行，例如“💬 …”。"},
        {"key": "content_type_line", "label": "内容类型这一行", "description": "会显示电影/电视剧和分类，例如“🏷️ 内容类型 ｜ 电视剧 · 动画 / 国漫”。"},
        {"key": "library_scope_line", "label": "更新信息这一行", "description": "单集会显示当前更新，电影会显示上映日期，整季会显示收录进度。"},
        {"key": "library_time_line", "label": "入库时间这一行", "description": "会显示“⏱️ 入库时间 ｜ 2026-07-06 15:19”这种格式。"},
        {"key": "quality_line", "label": "资源规格这一行", "description": "会把资源规格和文件数量整理成一行。"},
        {"key": "actors_line", "label": "主演阵容这一行", "description": "会把主演或配音阵容整理成一行。"},
        {"key": "title", "label": "标题", "description": "媒体主标题。"},
        {"key": "year", "label": "年份", "description": "作品年份。"},
        {"key": "year_suffix", "label": "年份后缀", "description": "如 （2026） 的展示后缀。"},
        {"key": "tagline", "label": "一句话简介", "description": "展示在标题下方的短简介。"},
        {"key": "rating", "label": "评分", "description": "整理后的评分值。"},
        {"key": "status", "label": "状态", "description": "如 已发布、整理完成。"},
        {"key": "processed_at", "label": "整理时间", "description": "发送通知时的处理时间。"},
        {"key": "media_type", "label": "媒体类型", "description": "电影、电视剧、动画等。"},
        {"key": "category", "label": "分类", "description": "类型或分类标签。"},
        {"key": "episode_info", "label": "入库信息", "description": "电影或集号信息。"},
        {"key": "latest_created_at", "label": "入库时间", "description": "最近文件创建时间。"},
        {"key": "quality", "label": "资源规格", "description": "分辨率、编码、码率等。"},
        {"key": "file_count", "label": "文件数量", "description": "本次入库文件数。"},
        {"key": "actors", "label": "主演阵容", "description": "主要演员或配音。"},
        {"key": "overview", "label": "内容简介", "description": "较长的正文简介。"},
    ]
    sample_payloads = {
        "playback.start": {
            "default": {
                "title_line": "🟢 【正在播放】云深不知梦 - S1, Ep1 - 特别篇：逐冥之役",
                "user_line": "🍿 播放用户：Alice",
                "playback_method_line": "📽️ 播放策略：直接播放 (Direct Play)",
                "media_spec_line": "🎟️ 媒体规格：2160p | H.265 | AAC",
                "rating_line": "▸ 评分：🌟 7.0 / 10",
                "content_type_line": "🎬类型：剧集",
                "headline": "🟢 【正在播放】云深不知梦 - S1, Ep1 - 特别篇：逐冥之役",
                "meta_line": "⭐ 评分：7.0/10 ｜ 📚 类型：剧集",
                "progress_line": "▸ 进度：🟢 00:03:24 / 00:24:00 (14%)",
                "ip_line": "▸ 网络：📍 203.0.113.8 上海",
                "device_line": "▸ 设备：📺 SenPlayer iPhone11",
                "occurred_at": "2026-07-03 21:15:08",
                "time_line": "▸ 时间：⏰ 2026-07-03 21:15:08",
                "overview_block": "📖 剧情简介：辉华族少女逐冥在逃亡途中再次卷入族地旧案，这一集揭开了她与逐冥之役的真正关联。",
                "overview_line": "📖 剧情简介：辉华族少女逐冥在逃亡途中再次卷入族地旧案，这一集揭开了她与逐冥之役的真正关联。",
                "user_name": "Alice",
                "title": "特别篇：逐冥之役",
                "action_text": "开始播放",
                "series_name": "云深不知梦",
                "episode_tag": "S01E01",
            }
        },
        "playback.pause": {
            "default": {
                "title_line": "🟡 【暂停播放】新电影 (2026)",
                "user_line": "🍿 播放用户：Alice",
                "playback_method_line": "📽️ 播放策略：直接播放 (Direct Play)",
                "media_spec_line": "🎟️ 媒体规格：1080p | H.264 | AAC",
                "rating_line": "▸ 评分：🌟 8.5 / 10",
                "content_type_line": "🎬类型：电影",
                "headline": "🟡 【暂停播放】新电影 (2026)",
                "meta_line": "⭐ 评分：8.5/10 ｜ 📚 类型：电影",
                "progress_line": "▸ 进度：🟡 00:54:10 / 02:03:00 (44%)",
                "ip_line": "▸ 网络：📍 203.0.113.8 上海",
                "device_line": "▸ 设备：📺 SenPlayer iPhone11",
                "occurred_at": "2026-07-03 21:15:08",
                "time_line": "▸ 时间：⏰ 2026-07-03 21:15:08",
                "overview_block": "📖 剧情简介：电影简介",
                "overview_line": "📖 剧情简介：电影简介",
                "user_name": "Alice",
                "title": "新电影",
                "action_text": "暂停播放",
                "series_name": "",
                "episode_tag": "",
            }
        },
        "playback.resume": {
            "default": {
                "title_line": "🟢 【恢复播放】新电影 (2026)",
                "user_line": "🍿 播放用户：Alice",
                "playback_method_line": "📽️ 播放策略：直接播放 (Direct Play)",
                "media_spec_line": "🎟️ 媒体规格：1080p | H.264 | AAC",
                "rating_line": "▸ 评分：🌟 8.5 / 10",
                "content_type_line": "🎬类型：电影",
                "headline": "🟢 【恢复播放】新电影 (2026)",
                "meta_line": "⭐ 评分：8.5/10 ｜ 📚 类型：电影",
                "progress_line": "▸ 进度：🟢 00:54:10 / 02:03:00 (44%)",
                "ip_line": "▸ 网络：📍 203.0.113.8 上海",
                "device_line": "▸ 设备：📺 SenPlayer iPhone11",
                "occurred_at": "2026-07-03 21:16:28",
                "time_line": "▸ 时间：⏰ 2026-07-03 21:16:28",
                "overview_block": "📖 剧情简介：电影简介",
                "overview_line": "📖 剧情简介：电影简介",
                "user_name": "Alice",
                "title": "新电影",
                "action_text": "恢复播放",
                "series_name": "",
                "episode_tag": "",
            }
        },
        "playback.stop": {
            "default": {
                "title_line": "🔴 【播放停止】新电影 (2026)",
                "user_line": "🍿 播放用户：Alice",
                "playback_method_line": "📽️ 播放策略：直接播放 (Direct Play)",
                "media_spec_line": "🎟️ 媒体规格：1080p | H.264 | AAC",
                "rating_line": "▸ 评分：🌟 8.5 / 10",
                "content_type_line": "🎬类型：电影",
                "headline": "🔴 【播放停止】新电影 (2026)",
                "meta_line": "⭐ 评分：8.5/10 ｜ 📚 类型：电影",
                "progress_line": "▸ 进度：🔴 02:03:00 / 02:03:00 (100%)",
                "ip_line": "▸ 网络：📍 203.0.113.8 上海",
                "device_line": "▸ 设备：📺 SenPlayer iPhone11",
                "occurred_at": "2026-07-03 23:00:08",
                "time_line": "▸ 时间：⏰ 2026-07-03 23:00:08",
                "overview_block": "📖 剧情简介：电影简介",
                "overview_line": "📖 剧情简介：电影简介",
                "user_name": "Alice",
                "title": "新电影",
                "action_text": "停止播放",
                "series_name": "",
                "episode_tag": "",
            }
        },
        "library.single": {
            "singleMovie": {
                "entry_label": "电影入库",
                "summary_line": "✨ 评分：8.5 / 10 ｜ 🔄 影片时长：02:08:00",
                "tagline_line": "💬 “欢迎来到真实世界。”",
                "content_type_line": "🏷️ 内容类型 ｜ 电影 · 剧情",
                "library_scope_line": "📅 上映日期 ｜ 2026-07-03",
                "library_time_line": "⏱️ 入库时间 ｜ 2026-07-03 21:14",
                "quality_line": "💿 资源规格 ｜ 4K / HDR / 12 Mbps (1 个文件)",
                "actors_line": "🤖 主演阵容 ｜ 主演甲",
                "title": "新电影",
                "year": "2026",
                "year_suffix": "（2026）",
                "tagline": "电影简介",
                "rating": "8.5",
                "status": "已发布",
                "processed_at": "2026-07-03 21:15:08",
                "media_type": "电影",
                "category": "剧情",
                "episode_info": "电影",
                "latest_created_at": "2026-07-03 21:14",
                "quality": "4K / HDR / 12 Mbps",
                "file_count": "1",
                "actors": "主演甲",
                "overview": "电影简介",
            },
            "singleEpisode": {
                "entry_label": "单集入库",
                "summary_line": "✨ 评分：7 / 10 ｜ 🔄 状态：整理完成",
                "tagline_line": "💬 “这一集揭开了逐冥之役的真正关联”",
                "content_type_line": "🏷️ 内容类型 ｜ 电视剧 · 动画 / 国漫",
                "library_scope_line": "▶️ 当前更新 ｜ S01E01",
                "library_time_line": "⏱️ 入库时间 ｜ 2026-07-03 21:14",
                "quality_line": "💿 资源规格 ｜ 1080p / AVC / 5 Mbps (1 个文件)",
                "actors_line": "🤖 主演阵容 ｜ 蔡海婷、张若瑜",
                "title": "云深不知梦",
                "year": "2026",
                "year_suffix": "（2026）",
                "tagline": "这一集揭开了逐冥之役的真正关联",
                "rating": "7",
                "status": "整理完成",
                "processed_at": "2026-07-03 21:15:08",
                "media_type": "电视剧",
                "category": "动画 / 国漫",
                "episode_info": "S01E01",
                "latest_created_at": "2026-07-03 21:14",
                "quality": "1080p / AVC / 5 Mbps",
                "file_count": "1",
                "actors": "蔡海婷、张若瑜",
                "overview": "辉华族少女逐冥在逃亡途中再次卷入族地旧案，这一集揭开了她与逐冥之役的真正关联。",
            },
        },
        "library.grouped": {
            "default": {
                "entry_label": "整季入库",
                "summary_line": "✨ 评分：7 / 10 ｜ 🔄 状态：整理完成",
                "tagline_line": "💬 “民国初年，南洋上发生水鬼望乡离奇命案。”",
                "content_type_line": "🏷️ 内容类型 ｜ 电视剧 · 华语剧集",
                "library_scope_line": "📑 收录进度 ｜ S01 E01-E03",
                "library_time_line": "⏱️ 入库时间 ｜ 2026-07-03 21:14",
                "quality_line": "💿 资源规格 ｜ 1080p / HEVC / 6 Mbps (3 个文件)",
                "actors_line": "🤖 主演阵容 ｜ 张新成、丁禹兮",
                "title": "南部档案",
                "year": "2026",
                "year_suffix": "（2026）",
                "tagline": "民国初年，南洋上发生水鬼望乡离奇命案。",
                "rating": "7",
                "status": "整理完成",
                "processed_at": "2026-07-03 21:15:08",
                "media_type": "电视剧",
                "category": "华语剧集",
                "episode_info": "S01 E01-E03",
                "latest_created_at": "2026-07-03 21:14",
                "quality": "1080p / HEVC / 6 Mbps",
                "file_count": "3",
                "actors": "张新成、丁禹兮",
                "overview": "民国初年，南洋上发生水鬼望乡离奇命案。",
            }
        },
    }
    event_meta = {
        "playback.start": (DEFAULT_EVENT_DISPLAY["playback.start"]["label"], DEFAULT_EVENT_DISPLAY["playback.start"]["description"], "playback", playback_variables),
        "playback.pause": (DEFAULT_EVENT_DISPLAY["playback.pause"]["label"], DEFAULT_EVENT_DISPLAY["playback.pause"]["description"], "playback", playback_variables),
        "playback.resume": (DEFAULT_EVENT_DISPLAY["playback.resume"]["label"], DEFAULT_EVENT_DISPLAY["playback.resume"]["description"], "playback", playback_variables),
        "playback.stop": (DEFAULT_EVENT_DISPLAY["playback.stop"]["label"], DEFAULT_EVENT_DISPLAY["playback.stop"]["description"], "playback", playback_variables),
        "library.single": (DEFAULT_EVENT_DISPLAY["library.single"]["label"], DEFAULT_EVENT_DISPLAY["library.single"]["description"], "library", library_variables),
        "library.grouped": (DEFAULT_EVENT_DISPLAY["library.grouped"]["label"], DEFAULT_EVENT_DISPLAY["library.grouped"]["description"], "library", library_variables),
    }
    events: list[dict[str, Any]] = []
    for key in REAL_EVENT_KEYS:
        label, description, category, variables = event_meta[key]
        events.append(
            {
                "key": key,
                "label": label,
                "description": description,
                "category": category,
                "supportedChannels": list(CHANNEL_KEYS),
                "connected": True,
                "variables": [dict(item) for item in variables],
                "samplePayloads": sample_payloads.get(key, {"default": {}}),
                "defaultTemplateByChannel": {
                    "telegram": defaults["telegram"].get(key, ""),
                    "wecom": defaults["wecom"].get(key, ""),
                },
            }
        )
    upcoming = [
        {
            "key": key,
            "label": {
                "organize.completed": "整理通知",
                "wash.completed": "洗版通知",
                "transfer.completed": "转存通知",
                "task.completed": "任务通知",
                "checkin.completed": "签到通知",
            }.get(key, key),
            "description": "事件源尚未接入，当前仅展示，不可启用。",
            "category": "future",
            "supportedChannels": list(CHANNEL_KEYS),
            "connected": False,
            "variables": [],
            "samplePayloads": {},
            "defaultTemplateByChannel": {"telegram": "", "wecom": ""},
        }
        for key in FUTURE_EVENT_KEYS
    ]
    return {
        "channels": {
            "telegram": {"label": "Telegram", "supportsCommands": True},
            "wecom": {"label": "企业微信", "supportsCommands": False},
        },
        "events": events,
        "upcomingEvents": upcoming,
    }


def build_notification_preview(
    *,
    channel: str,
    event_key: str,
    template: str,
    sample_key: str = "default",
    payload_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    capabilities = notification_capabilities()
    event = next((row for row in capabilities["events"] if row.get("key") == event_key), None)
    if not isinstance(event, dict):
        raise ValueError("未知通知事件")
    sample_payloads = event.get("samplePayloads") if isinstance(event.get("samplePayloads"), dict) else {}
    if sample_key not in sample_payloads:
        sample_key = next(iter(sample_payloads.keys()), "default")
    payload = sample_payloads.get(sample_key) if isinstance(sample_payloads.get(sample_key), dict) else {}
    if isinstance(payload_overrides, dict) and payload_overrides:
        next_payload = dict(payload)
        for key, value in payload_overrides.items():
            next_payload[str(key)] = value
        payload = next_payload
    preview = render_notification_template(template, payload)
    variables = extract_template_variables(template)
    missing = [key for key in variables if key not in payload]
    return {
        "channel": channel,
        "eventKey": event_key,
        "sampleKey": sample_key,
        "previewText": preview,
        "missingVariables": missing,
        "usedVariables": variables,
        "payload": payload,
    }


def route_enabled(config: dict[str, Any], channel: str, event_key: str) -> bool:
    if not bool(config.get("enabled", True)):
        return False
    channels = config.get("channels") if isinstance(config.get("channels"), dict) else {}
    channel_config = channels.get(channel) if isinstance(channels.get(channel), dict) else {}
    if not bool(channel_config.get("enabled", False)):
        return False
    routes = config.get("routes") if isinstance(config.get("routes"), dict) else {}
    route_map = routes.get(channel) if isinstance(routes.get(channel), dict) else {}
    return bool(route_map.get(event_key, False))


def any_route_enabled(config: dict[str, Any], event_keys: list[str] | tuple[str, ...]) -> bool:
    for channel in CHANNEL_KEYS:
        for event_key in event_keys:
            if route_enabled(config, channel, event_key):
                return True
    return False


class WecomNotificationSender:
    def __init__(self) -> None:
        self._ssl_ctx = ssl._create_unverified_context()

    def _build_opener(self, *, proxy_url: str = "") -> urllib.request.OpenerDirector:
        safe_proxy = str(proxy_url or "").strip()
        handlers: list[Any] = [urllib.request.HTTPSHandler(context=self._ssl_ctx)]
        if safe_proxy:
            handlers.append(urllib.request.ProxyHandler({"http": safe_proxy, "https": safe_proxy}))
        return urllib.request.build_opener(*handlers)

    def _open_json(self, request: urllib.request.Request, *, proxy_url: str = "") -> dict[str, Any]:
        opener = self._build_opener(proxy_url=proxy_url)
        try:
            with opener.open(request, timeout=20) as response:
                body = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as err:
            raw = err.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"企业微信请求失败（HTTP {err.code}）：{raw[:240]}") from err
        except urllib.error.URLError as err:
            reason = getattr(err, "reason", err)
            raise RuntimeError(f"企业微信网络错误：{reason}") from err
        try:
            parsed = json.loads(body)
        except Exception as err:
            raise RuntimeError(f"企业微信返回无法解析：{body[:240]}") from err
        if not isinstance(parsed, dict):
            raise RuntimeError("企业微信返回格式不正确")
        return parsed

    def get_access_token(self, *, corp_id: str, secret: str, proxy_url: str = "") -> str:
        query = urllib.parse.urlencode({"corpid": str(corp_id or "").strip(), "corpsecret": str(secret or "").strip()})
        request = urllib.request.Request(f"https://qyapi.weixin.qq.com/cgi-bin/gettoken?{query}", method="GET")
        parsed = self._open_json(request, proxy_url=proxy_url)
        if int(parsed.get("errcode") or 0) != 0:
            raise RuntimeError(str(parsed.get("errmsg") or "企业微信获取 access token 失败"))
        token = str(parsed.get("access_token") or "").strip()
        if not token:
            raise RuntimeError("企业微信 access token 为空")
        return token

    def send_text(
        self,
        *,
        corp_id: str,
        agent_id: str,
        secret: str,
        to_user: str,
        text: str,
        proxy_url: str = "",
    ) -> dict[str, Any]:
        access_token = self.get_access_token(corp_id=corp_id, secret=secret, proxy_url=proxy_url)
        payload = {
            "touser": str(to_user or "@all").strip() or "@all",
            "msgtype": "text",
            "agentid": int(str(agent_id or "0").strip() or "0"),
            "text": {"content": str(text or "")},
            "safe": 0,
        }
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={urllib.parse.quote(access_token, safe='')}",
            data=data,
            method="POST",
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        parsed = self._open_json(request, proxy_url=proxy_url)
        if int(parsed.get("errcode") or 0) != 0:
            raise RuntimeError(str(parsed.get("errmsg") or "企业微信发送失败"))
        return parsed


class NotificationDispatchService:
    def __init__(
        self,
        *,
        telegram_sender: TelegramSender | None = None,
        wecom_sender: WecomNotificationSender | None = None,
    ) -> None:
        self.telegram_sender = telegram_sender or TelegramSender()
        self.wecom_sender = wecom_sender or WecomNotificationSender()

    def send_test(self, *, config: dict[str, Any], channel: str) -> dict[str, Any]:
        safe_channel = str(channel or "").strip().lower()
        now_text = "2026-07-03 21:15:08"
        if safe_channel == "telegram":
            telegram = config["channels"]["telegram"]
            token = str(telegram.get("botToken") or "").strip()
            chat_id = str(telegram.get("chatId") or "").strip()
            if not token or not chat_id:
                raise ValueError("请先保存 Telegram Bot Token 和 Chat ID")
            text = "【VistaMirror】测试通知\n\n▸ 状态  ✅ 通道可用\n▸ 时间  🕐 " + now_text
            self.telegram_sender.send_text(token=token, chat_id=chat_id, text=text, proxy_url=str(telegram.get("proxyUrl") or "").strip())
            return {"ok": True, "detail": "Telegram 测试消息发送成功"}
        if safe_channel == "wecom":
            wecom = config["channels"]["wecom"]
            if not str(wecom.get("corpId") or "").strip() or not str(wecom.get("agentId") or "").strip() or not str(wecom.get("secret") or "").strip():
                raise ValueError("请先完整填写企业微信 Corp ID、Agent ID 和 Secret")
            text = "【VistaMirror】测试通知\n\n状态：通道可用\n时间：" + now_text
            self.wecom_sender.send_text(
                corp_id=str(wecom.get("corpId") or "").strip(),
                agent_id=str(wecom.get("agentId") or "").strip(),
                secret=str(wecom.get("secret") or "").strip(),
                to_user=str(wecom.get("toUser") or "@all").strip() or "@all",
                text=text,
                proxy_url=str(wecom.get("proxyUrl") or "").strip(),
            )
            return {"ok": True, "detail": "企业微信测试消息发送成功"}
        raise ValueError("未知通知通道")

    def dispatch(self, *, config: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
        safe_config = normalize_notification_config(config)
        event_key = str(event.get("eventKey") or "").strip()
        if event_key not in REAL_EVENT_KEYS:
            raise ValueError("未知通知事件")
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        channel_context = event.get("channelContext") if isinstance(event.get("channelContext"), dict) else {}
        results: list[dict[str, Any]] = []
        sent_count = 0
        skipped_count = 0
        for channel in CHANNEL_KEYS:
            if not route_enabled(safe_config, channel, event_key):
                results.append({"channel": channel, "status": "skipped", "reason": "route_disabled"})
                skipped_count += 1
                continue
            template = str(safe_config["templates"][channel].get(event_key) or "")
            text = render_notification_template(template, payload)
            if channel == "telegram":
                result = self._dispatch_telegram(
                    config=safe_config["channels"]["telegram"],
                    text=text,
                    context=channel_context.get("telegram") if isinstance(channel_context.get("telegram"), dict) else {},
                )
            else:
                result = self._dispatch_wecom(
                    config=safe_config["channels"]["wecom"],
                    text=text,
                )
            results.append({"channel": channel, **result})
            if result.get("status") == "sent":
                sent_count += 1
        return {
            "ok": sent_count > 0,
            "eventKey": event_key,
            "sentCount": sent_count,
            "skippedCount": skipped_count,
            "results": results,
        }

    def _dispatch_telegram(self, *, config: dict[str, Any], text: str, context: dict[str, Any]) -> dict[str, Any]:
        token = str(config.get("botToken") or "").strip()
        chat_id = str(config.get("chatId") or "").strip()
        if not token or not chat_id:
            return {"status": "skipped", "reason": "telegram_not_configured"}
        proxy_url = str(config.get("proxyUrl") or "").strip()
        photo_bytes = context.get("photoBytes") if isinstance(context.get("photoBytes"), (bytes, bytearray)) else b""
        photo_url = str(context.get("photoUrl") or "").strip()
        filename = str(context.get("filename") or "poster.jpg").strip() or "poster.jpg"
        content_type = str(context.get("contentType") or "image/jpeg").strip() or "image/jpeg"
        reply_markup = context.get("replyMarkup") if isinstance(context.get("replyMarkup"), dict) else None
        button_text = str(context.get("buttonText") or "").strip()
        button_url = str(context.get("buttonUrl") or "").strip()
        if reply_markup is None and button_text and button_url:
            reply_markup = {
                "inline_keyboard": [[{"text": button_text, "url": button_url}]],
            }
        if photo_bytes:
            try:
                self.telegram_sender.send_photo_file(
                    token=token,
                    chat_id=chat_id,
                    photo_bytes=bytes(photo_bytes),
                    caption=text,
                    filename=filename,
                    content_type=content_type,
                    reply_markup=reply_markup,
                    proxy_url=proxy_url,
                )
                return {"status": "sent", "mode": "photo_file"}
            except RuntimeError:
                self.telegram_sender.send_text(
                    token=token,
                    chat_id=chat_id,
                    text=text,
                    reply_markup=reply_markup,
                    proxy_url=proxy_url,
                )
                return {"status": "sent", "mode": "text_fallback"}
        if photo_url:
            try:
                self.telegram_sender.send_photo(
                    token=token,
                    chat_id=chat_id,
                    photo_url=photo_url,
                    caption=text,
                    button_text=button_text,
                    button_url=button_url,
                    proxy_url=proxy_url,
                )
                return {"status": "sent", "mode": "photo_url"}
            except RuntimeError as err:
                lowered = str(err).lower()
                should_fallback = any(key in lowered for key in ("wrong type", "http url", "wrong file identifier", "bad request"))
                if not should_fallback:
                    raise
        self.telegram_sender.send_text(
            token=token,
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            proxy_url=proxy_url,
        )
        return {"status": "sent", "mode": "text"}

    def _dispatch_wecom(self, *, config: dict[str, Any], text: str) -> dict[str, Any]:
        corp_id = str(config.get("corpId") or "").strip()
        agent_id = str(config.get("agentId") or "").strip()
        secret = str(config.get("secret") or "").strip()
        if not corp_id or not agent_id or not secret:
            return {"status": "skipped", "reason": "wecom_not_configured"}
        self.wecom_sender.send_text(
            corp_id=corp_id,
            agent_id=agent_id,
            secret=secret,
            to_user=str(config.get("toUser") or "@all").strip() or "@all",
            text=text,
            proxy_url=str(config.get("proxyUrl") or "").strip(),
        )
        return {"status": "sent", "mode": "text"}
