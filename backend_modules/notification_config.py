from __future__ import annotations

import copy
import re
from typing import Any

TELEGRAM_CHAT_ID_PATTERN = re.compile(r"^-?\d+$")


def default_bot_config() -> dict[str, Any]:
    return {
        "enableCore": True,
        "enablePlayback": True,
        "enableLibrary": True,
        "telegramToken": "",
        "telegramChatId": "",
        "wechatCorpId": "",
        "wechatAgentId": "",
        "wechatSecret": "",
        "wechatToUser": "@all",
        "wechatCallbackToken": "",
        "wechatCallbackAes": "",
        "enableCommands": True,
        "notifyEvents": {
            "start": True,
            "pause": True,
            "resume": True,
            "stop": True,
        },
        "showIp": True,
        "showIpGeo": True,
        "showOverview": True,
        "eventDedupSeconds": 10,
    }


def normalize_notify_events(raw: Any, defaults: dict[str, Any]) -> dict[str, bool]:
    source = raw if isinstance(raw, dict) else {}
    result: dict[str, bool] = {}
    for key in ("start", "pause", "resume", "stop"):
        result[key] = bool(source.get(key, defaults.get(key, True)))
    return result


def normalize_bot_config(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    defaults = default_bot_config()
    notify_events = normalize_notify_events(source.get("notifyEvents"), defaults["notifyEvents"])
    try:
        dedupe_seconds = int(source.get("eventDedupSeconds", defaults["eventDedupSeconds"]))
    except (TypeError, ValueError):
        dedupe_seconds = int(defaults["eventDedupSeconds"])
    dedupe_seconds = max(1, min(120, dedupe_seconds))
    return {
        "enableCore": bool(source.get("enableCore", defaults["enableCore"])),
        "enablePlayback": bool(source.get("enablePlayback", defaults["enablePlayback"])),
        "enableLibrary": bool(source.get("enableLibrary", defaults["enableLibrary"])),
        "telegramToken": str(source.get("telegramToken") or "").strip(),
        "telegramChatId": str(source.get("telegramChatId") or "").strip(),
        "wechatCorpId": str(source.get("wechatCorpId") or "").strip(),
        "wechatAgentId": str(source.get("wechatAgentId") or "").strip(),
        "wechatSecret": str(source.get("wechatSecret") or "").strip(),
        "wechatToUser": str(source.get("wechatToUser") or defaults["wechatToUser"]).strip() or defaults["wechatToUser"],
        "wechatCallbackToken": str(source.get("wechatCallbackToken") or "").strip(),
        "wechatCallbackAes": str(source.get("wechatCallbackAes") or "").strip(),
        "enableCommands": bool(source.get("enableCommands", defaults["enableCommands"])),
        "notifyEvents": notify_events,
        "showIp": bool(source.get("showIp", defaults["showIp"])),
        "showIpGeo": bool(source.get("showIpGeo", defaults["showIpGeo"])),
        "showOverview": bool(source.get("showOverview", defaults["showOverview"])),
        "eventDedupSeconds": dedupe_seconds,
    }


def validate_bot_config(raw: Any) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(raw, dict):
        return None, "botConfig 必须是对象"

    config = normalize_bot_config(raw)
    token = config.get("telegramToken", "")
    chat_id = config.get("telegramChatId", "")

    if (token and not chat_id) or (chat_id and not token):
        return None, "Telegram Token 与 Chat ID 需要同时填写或同时留空"
    if token and ":" not in token:
        return None, "Telegram Token 格式看起来不正确"
    if chat_id and not TELEGRAM_CHAT_ID_PATTERN.match(chat_id):
        return None, "Telegram Chat ID 格式不正确，应为数字"

    if not isinstance(config.get("notifyEvents"), dict):
        return None, "notifyEvents 必须是对象"
    return config, None


def deepcopy_default_bot_config() -> dict[str, Any]:
    return copy.deepcopy(default_bot_config())
