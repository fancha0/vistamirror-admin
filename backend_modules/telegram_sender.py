from __future__ import annotations

import json
import os
import ssl
import urllib.error
import urllib.request
from typing import Any


class TelegramSender:
    def __init__(self) -> None:
        self._ssl_ctx = ssl._create_unverified_context()

    def _build_opener(self) -> urllib.request.OpenerDirector:
        proxy_url = str(os.environ.get("TG_PROXY_URL") or "").strip()
        handlers: list[Any] = [urllib.request.HTTPSHandler(context=self._ssl_ctx)]
        if not proxy_url:
            return urllib.request.build_opener(*handlers)
        handlers.append(
            urllib.request.ProxyHandler(
                {
                    "http": proxy_url,
                    "https": proxy_url,
                }
            )
        )
        return urllib.request.build_opener(*handlers)

    def api_request(self, *, token: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        safe_token = str(token or "").strip()
        safe_method = str(method or "").strip()
        if not safe_token or not safe_method:
            raise ValueError("Telegram 请求参数不完整")
        url = f"https://api.telegram.org/bot{safe_token}/{safe_method}"
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        opener = self._build_opener()
        try:
            with opener.open(request, timeout=20) as response:
                body = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as err:
            raw = err.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Telegram 请求失败（HTTP {err.code}）：{raw[:240]}") from err
        except urllib.error.URLError as err:
            reason = getattr(err, "reason", err)
            raise RuntimeError(f"Telegram 网络错误：{reason}") from err
        try:
            parsed = json.loads(body)
        except Exception as err:
            raise RuntimeError(f"Telegram 返回无法解析：{body[:240]}") from err
        if not isinstance(parsed, dict) or not parsed.get("ok"):
            desc = ""
            if isinstance(parsed, dict):
                desc = str(parsed.get("description") or parsed.get("error") or "").strip()
            raise RuntimeError(f"Telegram 请求失败：{desc or body[:240]}")
        return parsed

    def send_text(self, *, token: str, chat_id: str, text: str) -> None:
        self.api_request(
            token=token,
            method="sendMessage",
            payload={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
        )

    def send_photo(
        self,
        *,
        token: str,
        chat_id: str,
        photo_url: str,
        caption: str,
        button_text: str = "",
        button_url: str = "",
    ) -> None:
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "photo": photo_url,
            "caption": caption,
            "disable_web_page_preview": True,
        }
        if button_text and button_url:
            payload["reply_markup"] = {
                "inline_keyboard": [[{"text": button_text, "url": button_url}]],
            }
        self.api_request(token=token, method="sendPhoto", payload=payload)

    def set_my_commands(self, *, token: str, commands: list[dict[str, str]]) -> None:
        self.api_request(token=token, method="setMyCommands", payload={"commands": commands})

    def get_updates(self, *, token: str, offset: int, timeout_seconds: int = 25) -> list[dict[str, Any]]:
        payload = {"offset": offset, "timeout": max(5, min(60, int(timeout_seconds or 25)))}
        resp = self.api_request(token=token, method="getUpdates", payload=payload)
        result = resp.get("result")
        if isinstance(result, list):
            return [row for row in result if isinstance(row, dict)]
        return []
