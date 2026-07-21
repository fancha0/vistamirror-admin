from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse


DEFAULT_MOVIEPILOT_CONFIG: dict[str, Any] = {
    "enabled": False,
    "baseUrl": "",
    "apiToken": "",
    "timeoutSeconds": 12,
}

_ENV_FIELDS = {
    "enabled": "MOVIEPILOT_ENABLED",
    "baseUrl": "MOVIEPILOT_BASE_URL",
    "apiToken": "MOVIEPILOT_API_TOKEN",
    "timeoutSeconds": "MOVIEPILOT_TIMEOUT_SECONDS",
}


def default_moviepilot_config() -> dict[str, Any]:
    return dict(DEFAULT_MOVIEPILOT_CONFIG)


def _to_bool(value: Any, fallback: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return fallback
    return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _to_timeout(value: Any) -> int:
    try:
        timeout = int(value)
    except (TypeError, ValueError):
        timeout = int(DEFAULT_MOVIEPILOT_CONFIG["timeoutSeconds"])
    return max(3, min(60, timeout))


def normalize_moviepilot_config(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    return {
        "enabled": _to_bool(source.get("enabled"), bool(DEFAULT_MOVIEPILOT_CONFIG["enabled"])),
        "baseUrl": str(source.get("baseUrl") or "").strip().rstrip("/"),
        "apiToken": str(source.get("apiToken") or "").strip(),
        "timeoutSeconds": _to_timeout(source.get("timeoutSeconds")),
    }


def env_managed_moviepilot_fields() -> list[str]:
    return [field for field, env_name in _ENV_FIELDS.items() if str(os.getenv(env_name) or "").strip()]


def apply_moviepilot_env_overrides(raw: Any) -> dict[str, Any]:
    config = normalize_moviepilot_config(raw)
    if str(os.getenv(_ENV_FIELDS["enabled"]) or "").strip():
        config["enabled"] = _to_bool(os.getenv(_ENV_FIELDS["enabled"]))
    if str(os.getenv(_ENV_FIELDS["baseUrl"]) or "").strip():
        config["baseUrl"] = str(os.getenv(_ENV_FIELDS["baseUrl"]) or "").strip().rstrip("/")
    if str(os.getenv(_ENV_FIELDS["apiToken"]) or "").strip():
        config["apiToken"] = str(os.getenv(_ENV_FIELDS["apiToken"]) or "").strip()
    if str(os.getenv(_ENV_FIELDS["timeoutSeconds"]) or "").strip():
        config["timeoutSeconds"] = _to_timeout(os.getenv(_ENV_FIELDS["timeoutSeconds"]))
    return config


def merge_moviepilot_config_for_save(current: Any, raw: Any) -> dict[str, Any]:
    previous = normalize_moviepilot_config(current)
    incoming = raw if isinstance(raw, dict) else {}
    merged = {**previous, **incoming}
    # The browser never receives the saved token, so an omitted/empty token keeps it intact.
    if not str(incoming.get("apiToken") or "").strip():
        merged["apiToken"] = previous["apiToken"]
    return normalize_moviepilot_config(merged)


def validate_moviepilot_config(raw: Any) -> tuple[dict[str, Any], str]:
    config = normalize_moviepilot_config(raw)
    if not config["enabled"]:
        return config, ""
    if not config["baseUrl"]:
        return config, "请填写 MoviePilot 地址。"
    parsed = urlparse(config["baseUrl"])
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return config, "MoviePilot 地址必须是有效的 http:// 或 https:// 地址。"
    if not config["apiToken"]:
        return config, "请填写 MoviePilot API Token。"
    return config, ""


def public_moviepilot_config(raw: Any) -> dict[str, Any]:
    config = normalize_moviepilot_config(raw)
    return {
        "enabled": config["enabled"],
        "baseUrl": config["baseUrl"],
        "timeoutSeconds": config["timeoutSeconds"],
        "hasApiToken": bool(config["apiToken"]),
    }
