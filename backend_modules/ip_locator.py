from __future__ import annotations

import ipaddress
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

_IP_GEO_CACHE: dict[str, str] = {}
_GEO_LOOKUP_TIMEOUT = 2.0


def parse_ip_from_endpoint(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if value.startswith("[") and "]:" in value:
        return value[1 : value.find("]")]
    if value.count(":") == 1 and "." in value:
        host, _, _ = value.partition(":")
        return host.strip()
    return value


def is_public_ip(ip_text: str) -> bool:
    text = str(ip_text or "").strip()
    if not text:
        return False
    try:
        ip = ipaddress.ip_address(text)
    except ValueError:
        return False
    return not (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved)


def _normalize_geo_token(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text:
        return ""
    text = text.replace("中国", "").strip()
    suffixes = (
        "维吾尔自治区",
        "壮族自治区",
        "回族自治区",
        "特别行政区",
        "自治区",
        "省",
        "市",
    )
    for suffix in suffixes:
        if text.endswith(suffix):
            text = text[: -len(suffix)].strip()
            break
    return text


def _join_geo_parts(*parts: Any) -> str:
    rows: list[str] = []
    for value in parts:
        token = _normalize_geo_token(value)
        if token and token not in rows:
            rows.append(token)
    return " ".join(rows).strip()


def _decode_response_bytes(raw: bytes) -> str:
    for encoding in ("utf-8", "utf-8-sig", "gbk", "gb18030"):
        try:
            return raw.decode(encoding)
        except Exception:
            continue
    return raw.decode("utf-8", errors="ignore")


def _load_json_from_url(url: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "VistaMirror/1.0",
            "Accept": "application/json,text/plain,*/*",
        },
    )
    with urllib.request.urlopen(request, timeout=_GEO_LOOKUP_TIMEOUT) as response:
        text = _decode_response_bytes(response.read()).strip()
    if not text:
        return {}
    if text.startswith(("callback(", "jsonp(")) and text.endswith(")"):
        start = text.find("(") + 1
        end = text.rfind(")")
        text = text[start:end].strip()
    if not text.startswith("{"):
        match = re.search(r"(\{.*\})", text, re.S)
        if match:
            text = match.group(1)
    try:
        data = json.loads(text)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _lookup_geo_from_pconline(ip_text: str) -> str:
    query = urllib.parse.urlencode({"json": "true", "ip": ip_text})
    data = _load_json_from_url(f"https://whois.pconline.com.cn/ipJson.jsp?{query}")
    return _join_geo_parts(data.get("pro"), data.get("city"))


def _lookup_geo_from_ipinfo(ip_text: str) -> str:
    data = _load_json_from_url(f"https://ipinfo.io/{urllib.parse.quote(ip_text, safe='')}/json")
    return _join_geo_parts(data.get("region"), data.get("city"))


def extract_geo_from_payload(payload: dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""
    for source in (payload, payload.get("Session"), payload.get("session")):
        if not isinstance(source, dict):
            continue
        for key in ("Geo", "geo", "IpLocation", "ipLocation", "Location", "location", "Area", "area", "Address", "address"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return re.sub(r"\s+", " ", value.strip())
    return ""


def lookup_geo_for_ip(ip_text: str) -> str:
    safe_ip = str(ip_text or "").strip()
    if not safe_ip or not is_public_ip(safe_ip):
        return ""
    if safe_ip in _IP_GEO_CACHE:
        return _IP_GEO_CACHE[safe_ip]
    geo = ""
    for resolver in (_lookup_geo_from_pconline, _lookup_geo_from_ipinfo):
        try:
            geo = resolver(safe_ip)
        except (urllib.error.URLError, TimeoutError, ValueError, OSError):
            geo = ""
        if geo:
            break
    _IP_GEO_CACHE[safe_ip] = geo
    return geo


def build_ip_display(payload: dict[str, Any], *, show_ip: bool, show_geo: bool) -> str:
    if not show_ip:
        return ""
    candidates: list[str] = []
    for key in ("RemoteEndPoint", "remoteEndPoint", "IpAddress", "ipAddress"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            candidates.append(value.strip())
    session = payload.get("Session")
    if isinstance(session, dict):
        for key in ("RemoteEndPoint", "remoteEndPoint", "IpAddress", "ipAddress"):
            value = session.get(key)
            if isinstance(value, str) and value.strip():
                candidates.append(value.strip())
    ip_text = parse_ip_from_endpoint(candidates[0] if candidates else "")
    if not ip_text:
        return ""
    if not show_geo:
        return ip_text
    geo = extract_geo_from_payload(payload)
    if not geo:
        geo = lookup_geo_for_ip(ip_text)
    if geo and geo not in ip_text:
        return f"{ip_text} {geo}"
    return ip_text
