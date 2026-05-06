from __future__ import annotations

import ipaddress
import re
from typing import Any


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


def extract_geo_from_payload(payload: dict[str, Any]) -> str:
    for key in ("Geo", "geo", "IpLocation", "ipLocation", "Location", "location", "Area", "area", "Address", "address"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return re.sub(r"\s+", " ", value.strip())
    return ""


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
    if geo and geo not in ip_text:
        return f"{ip_text} {geo}"
    return ip_text
