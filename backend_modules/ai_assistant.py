from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Iterator


AI_ENV_FIELD_MAP: dict[str, str] = {
    "baseUrl": "APP_AI_BASE_URL",
    "apiKey": "APP_AI_API_KEY",
    "model": "APP_AI_MODEL",
}


def default_ai_config() -> dict[str, Any]:
    return {
        "enabled": False,
        "baseUrl": "https://api.openai.com/v1",
        "apiKey": "",
        "model": "gpt-4o-mini",
        "temperature": 0.4,
        "maxTokens": 800,
        "contextTokensK": 64,
    }


def normalize_ai_config(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    defaults = default_ai_config()
    try:
        temperature = float(source.get("temperature", defaults["temperature"]))
    except (TypeError, ValueError):
        temperature = float(defaults["temperature"])
    temperature = max(0.0, min(2.0, temperature))
    try:
        max_tokens = int(source.get("maxTokens", defaults["maxTokens"]))
    except (TypeError, ValueError):
        max_tokens = int(defaults["maxTokens"])
    max_tokens = max(128, min(4000, max_tokens))
    try:
        context_tokens_k = int(source.get("contextTokensK", defaults["contextTokensK"]))
    except (TypeError, ValueError):
        context_tokens_k = int(defaults["contextTokensK"])
    context_tokens_k = max(4, min(1024, context_tokens_k))
    return {
        "enabled": bool(source.get("enabled", defaults["enabled"])),
        "baseUrl": str(source.get("baseUrl") or defaults["baseUrl"]).strip().rstrip("/"),
        "apiKey": str(source.get("apiKey") or "").strip(),
        "model": str(source.get("model") or defaults["model"]).strip() or defaults["model"],
        "temperature": temperature,
        "maxTokens": max_tokens,
        "contextTokensK": context_tokens_k,
    }


def env_managed_ai_fields() -> list[str]:
    managed: list[str] = []
    for field, env_name in AI_ENV_FIELD_MAP.items():
        if str(os.environ.get(env_name) or "").strip():
            managed.append(field)
    return managed


def apply_ai_env_overrides(raw: Any) -> dict[str, Any]:
    merged = normalize_ai_config(raw)
    for field, env_name in AI_ENV_FIELD_MAP.items():
        value = str(os.environ.get(env_name) or "").strip()
        if value:
            merged[field] = value.rstrip("/") if field == "baseUrl" else value
    return normalize_ai_config(merged)


def validate_ai_config(raw: Any) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(raw, dict):
        return None, "AI 配置必须是对象"
    config = normalize_ai_config(raw)
    if config["enabled"]:
        if not config["baseUrl"]:
            return None, "启用 AI 后必须填写 Base URL"
        if not config["apiKey"]:
            return None, "启用 AI 后必须填写 API Key"
        if not config["model"]:
            return None, "启用 AI 后必须填写模型名称"
        parsed = urllib.parse.urlsplit(config["baseUrl"])
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return None, "AI Base URL 必须是 http 或 https 地址"
    return config, None


def mask_ai_config(config: dict[str, Any]) -> dict[str, Any]:
    masked = dict(normalize_ai_config(config))
    raw = str(masked.get("apiKey") or "")
    if raw:
        masked["apiKey"] = f"{raw[:4]}***{raw[-4:]}" if len(raw) > 8 else "***"
    return masked


def chat_completion(
    *,
    config: dict[str, Any],
    messages: list[dict[str, str]],
    timeout_seconds: int = 45,
) -> str:
    cfg = apply_ai_env_overrides(config)
    if not cfg.get("enabled"):
        raise RuntimeError("AI 助手未启用")
    if not cfg.get("baseUrl") or not cfg.get("apiKey") or not cfg.get("model"):
        raise RuntimeError("AI 配置不完整")

    url = f"{str(cfg['baseUrl']).rstrip('/')}/chat/completions"
    payload = {
        "model": cfg["model"],
        "messages": messages,
        "temperature": cfg["temperature"],
        "max_tokens": cfg["maxTokens"],
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {cfg['apiKey']}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=max(5, int(timeout_seconds))) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as err:
        error_body = err.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"AI 请求失败：HTTP {err.code} {_extract_error(error_body)}") from None
    except Exception as err:
        raise RuntimeError(f"AI 请求失败：{err}") from None

    try:
        parsed = json.loads(raw)
    except Exception:
        raise RuntimeError("AI 返回内容不是有效 JSON") from None
    choices = parsed.get("choices") if isinstance(parsed, dict) else None
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("AI 返回结果为空")
    message = choices[0].get("message") if isinstance(choices[0], dict) else {}
    content = str(message.get("content") or "").strip() if isinstance(message, dict) else ""
    if not content:
        raise RuntimeError("AI 返回内容为空")
    return content


def stream_chat_completion(
    *,
    config: dict[str, Any],
    messages: list[dict[str, str]],
    timeout_seconds: int = 45,
) -> Iterator[str]:
    cfg = apply_ai_env_overrides(config)
    if not cfg.get("enabled"):
        raise RuntimeError("AI 助手未启用")
    if not cfg.get("baseUrl") or not cfg.get("apiKey") or not cfg.get("model"):
        raise RuntimeError("AI 配置不完整")

    url = f"{str(cfg['baseUrl']).rstrip('/')}/chat/completions"
    payload = {
        "model": cfg["model"],
        "messages": messages,
        "temperature": cfg["temperature"],
        "max_tokens": cfg["maxTokens"],
        "stream": True,
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {cfg['apiKey']}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        },
    )
    try:
        response = urllib.request.urlopen(request, timeout=max(5, int(timeout_seconds)))
    except urllib.error.HTTPError as err:
        error_body = err.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"AI 流式请求失败：HTTP {err.code} {_extract_error(error_body)}") from None
    except Exception as err:
        raise RuntimeError(f"AI 流式请求失败：{err}") from None

    yielded = False
    try:
        with response:
            for raw_line in response:
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line or line.startswith(":"):
                    continue
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if data == "[DONE]":
                    return
                try:
                    parsed = json.loads(data)
                except Exception:
                    continue
                if isinstance(parsed, dict) and isinstance(parsed.get("error"), dict):
                    raise RuntimeError(_extract_error(json.dumps(parsed, ensure_ascii=False)))
                choices = parsed.get("choices") if isinstance(parsed, dict) else None
                if not isinstance(choices, list) or not choices:
                    continue
                choice = choices[0] if isinstance(choices[0], dict) else {}
                delta = choice.get("delta") if isinstance(choice.get("delta"), dict) else {}
                content = str(delta.get("content") or "")
                if content:
                    yielded = True
                    yield content
    except RuntimeError:
        raise
    except Exception as err:
        raise RuntimeError(f"AI 流式读取失败：{err}") from None
    if not yielded:
        raise RuntimeError("AI 流式返回内容为空")


def _extract_error(raw: str) -> str:
    try:
        parsed = json.loads(raw)
    except Exception:
        return str(raw or "").strip()[:240]
    if isinstance(parsed, dict):
        error = parsed.get("error")
        if isinstance(error, dict):
            msg = str(error.get("message") or "").strip()
            if msg:
                return msg[:240]
        for key in ("message", "detail", "description"):
            msg = str(parsed.get(key) or "").strip()
            if msg:
                return msg[:240]
    return str(raw or "").strip()[:240]
