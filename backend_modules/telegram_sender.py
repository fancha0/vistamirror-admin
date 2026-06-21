from __future__ import annotations

import json
import os
import ssl
import urllib.error
import urllib.request
import uuid
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

    def multipart_request(
        self,
        *,
        token: str,
        method: str,
        fields: dict[str, Any],
        files: dict[str, tuple[str, bytes, str]],
    ) -> dict[str, Any]:
        safe_token = str(token or "").strip()
        safe_method = str(method or "").strip()
        if not safe_token or not safe_method:
            raise ValueError("Telegram 请求参数不完整")

        boundary = f"----vistamirror-{uuid.uuid4().hex}"
        chunks: list[bytes] = []
        for key, value in fields.items():
            chunks.extend(
                [
                    f"--{boundary}\r\n".encode("utf-8"),
                    f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode("utf-8"),
                    str(value).encode("utf-8"),
                    b"\r\n",
                ]
            )
        for key, (filename, content, content_type) in files.items():
            chunks.extend(
                [
                    f"--{boundary}\r\n".encode("utf-8"),
                    f'Content-Disposition: form-data; name="{key}"; filename="{filename}"\r\n'.encode("utf-8"),
                    f"Content-Type: {content_type or 'application/octet-stream'}\r\n\r\n".encode("utf-8"),
                    content,
                    b"\r\n",
                ]
            )
        chunks.append(f"--{boundary}--\r\n".encode("utf-8"))
        data = b"".join(chunks)

        request = urllib.request.Request(
            f"https://api.telegram.org/bot{safe_token}/{safe_method}",
            data=data,
            method="POST",
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        opener = self._build_opener()
        try:
            with opener.open(request, timeout=30) as response:
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

    def send_text(
        self,
        *,
        token: str,
        chat_id: str,
        text: str,
        reply_markup: dict[str, Any] | None = None,
        parse_mode: str = "",
        reply_to_message_id: int = 0,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if reply_markup:
            payload["reply_markup"] = reply_markup
        if int(reply_to_message_id or 0) > 0:
            payload["reply_parameters"] = {
                "message_id": int(reply_to_message_id),
                "allow_sending_without_reply": True,
            }
        return self.api_request(token=token, method="sendMessage", payload=payload)

    def edit_message_text(
        self,
        *,
        token: str,
        chat_id: str,
        message_id: int,
        text: str,
        reply_markup: dict[str, Any] | None = None,
        parse_mode: str = "",
    ) -> None:
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if reply_markup:
            payload["reply_markup"] = reply_markup
        self.api_request(token=token, method="editMessageText", payload=payload)

    def answer_callback_query(self, *, token: str, callback_query_id: str, text: str = "") -> None:
        payload: dict[str, Any] = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
            payload["show_alert"] = False
        self.api_request(token=token, method="answerCallbackQuery", payload=payload)

    def send_chat_action(self, *, token: str, chat_id: str, action: str = "typing") -> None:
        payload = {"chat_id": chat_id, "action": action or "typing"}
        self.api_request(token=token, method="sendChatAction", payload=payload)

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
        }
        if button_text and button_url:
            payload["reply_markup"] = {
                "inline_keyboard": [[{"text": button_text, "url": button_url}]],
            }
        self.api_request(token=token, method="sendPhoto", payload=payload)

    def send_photo_file(
        self,
        *,
        token: str,
        chat_id: str,
        photo_bytes: bytes,
        caption: str,
        filename: str = "poster.jpg",
        content_type: str = "image/jpeg",
        reply_markup: dict[str, Any] | None = None,
    ) -> None:
        fields: dict[str, Any] = {
            "chat_id": chat_id,
            "caption": caption,
        }
        if reply_markup:
            fields["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
        self.multipart_request(
            token=token,
            method="sendPhoto",
            fields=fields,
            files={"photo": (filename or "poster.jpg", photo_bytes, content_type or "image/jpeg")},
        )

    def set_my_commands(self, *, token: str, commands: list[dict[str, str]]) -> None:
        self.api_request(token=token, method="setMyCommands", payload={"commands": commands})

    def get_updates(self, *, token: str, offset: int, timeout_seconds: int = 25) -> list[dict[str, Any]]:
        payload = {"offset": offset, "timeout": max(5, min(60, int(timeout_seconds or 25)))}
        resp = self.api_request(token=token, method="getUpdates", payload=payload)
        result = resp.get("result")
        if isinstance(result, list):
            return [row for row in result if isinstance(row, dict)]
        return []
