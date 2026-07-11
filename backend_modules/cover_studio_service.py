from __future__ import annotations

import base64
from dataclasses import dataclass
from io import BytesIO
import json
import math
import os
import pathlib
import random
import re
import secrets
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from PIL import Image, ImageDraw, ImageFilter, ImageFont

from backend_modules.cover_studio_template_specs import get_cinematic_showcase_variant


DEFAULT_CANVAS_SIZE = (1600, 900)
DEFAULT_THUMB_SIZE = (1280, 720)
PREVIEW_TTL_SECONDS = 60 * 60 * 6
DEFAULT_TEMPLATE_KEY = "fan_spread"
DEFAULT_COVER_STUDIO_CRON = "0 */6 * * *"
ALL_TEMPLATE_SUPPORTS = [
    "titleAlign",
    "posterCount",
    "accentTone",
    "posterRotation",
    "titleYOffset",
]
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_CUSTOM_COVER_FONT_DIR = PROJECT_ROOT / "runtime" / "assets" / "fonts" / "custom"
CUSTOM_COVER_FONT_EXTENSIONS = {".ttf", ".otf", ".ttc"}


def cover_studio_modes() -> list[dict[str, Any]]:
    return [
        {
            "key": "fan_spread",
            "label": "扇形展开",
            "description": "多张海报像扇面一样展开，适合动画和剧集类视图。",
            "supports": list(ALL_TEMPLATE_SUPPORTS),
            "defaults": {
                "titleAlign": "left",
                "overlayStrength": 0,
                "posterCount": 6,
                "accentTone": "gold",
                "posterRotation": 68,
                "titleYOffset": -12,
                },
        },
        {
            "key": "banner_showcase",
            "label": "横幅橱窗",
            "description": "大背景主视觉加底部海报陈列，适合做流媒体风格分类封面。",
            "supports": list(ALL_TEMPLATE_SUPPORTS),
            "family": "cinematic_showcase",
            "variant": "banner",
            "defaults": {
                "titleAlign": "left",
                "overlayStrength": 0,
                "posterCount": 5,
                "accentTone": "gold",
                "posterRotation": 18,
                "titleYOffset": 0,
            },
        },
        {
            "key": "hero_showcase",
            "label": "主视觉橱窗",
            "description": "强化主视觉人物与灯光层次，适合剧集与国漫的首页封面。",
            "supports": list(ALL_TEMPLATE_SUPPORTS),
            "family": "cinematic_showcase",
            "variant": "hero",
            "defaults": {
                "titleAlign": "left",
                "overlayStrength": 0,
                "posterCount": 5,
                "accentTone": "blue",
                "posterRotation": 12,
                "titleYOffset": 0,
            },
        },
        {
            "key": "gallery_wall_showcase",
            "label": "海报陈列墙",
            "description": "用更规整的海报橱窗形成分类感，适合电影库与动漫库封面。",
            "supports": list(ALL_TEMPLATE_SUPPORTS),
            "family": "cinematic_showcase",
            "variant": "gallery",
            "defaults": {
                "titleAlign": "left",
                "overlayStrength": 0,
                "posterCount": 6,
                "accentTone": "emerald",
                "posterRotation": 8,
                "titleYOffset": 0,
            },
        },
        {
            "key": "immersive_stage",
            "label": "沉浸展映台",
            "description": "深色影院舞台感与倒影灯光更强，适合突出沉浸式流媒体封面。",
            "supports": list(ALL_TEMPLATE_SUPPORTS),
            "family": "cinematic_showcase",
            "variant": "immersive",
            "defaults": {
                "titleAlign": "left",
                "overlayStrength": 0,
                "posterCount": 5,
                "accentTone": "rose",
                "posterRotation": 16,
                "titleYOffset": 0,
            },
        },
    ]


def cover_studio_accent_tones() -> list[dict[str, str]]:
    return [
        {"key": "blue", "label": "海蓝"},
        {"key": "gold", "label": "鎏金"},
        {"key": "emerald", "label": "翡翠"},
        {"key": "rose", "label": "玫瑰"},
        {"key": "neutral", "label": "冷灰"},
    ]


def cover_studio_title_align_options() -> list[dict[str, str]]:
    return [
        {"key": "left", "label": "左对齐"},
        {"key": "center", "label": "居中"},
        {"key": "right", "label": "右对齐"},
    ]


def default_cover_studio_config() -> dict[str, Any]:
    mode_defaults = _mode_defaults(DEFAULT_TEMPLATE_KEY)
    return {
        "currentPresetId": "default",
        "lastViewId": "",
        "draft": {
            "viewId": "",
            "viewIds": [],
            "templateKey": DEFAULT_TEMPLATE_KEY,
            "pickMode": "random",
            "titleText": "",
            "subtitleText": "",
            "fontKey": "hiragino",
            "titleFontSize": 108,
            "subtitleFontSize": 44,
            "presetName": "默认封面",
            "titleAlign": mode_defaults["titleAlign"],
            "overlayStrength": mode_defaults["overlayStrength"],
            "posterCount": mode_defaults["posterCount"],
            "accentTone": mode_defaults["accentTone"],
            "posterRotation": mode_defaults["posterRotation"],
            "titleYOffset": mode_defaults["titleYOffset"],
            "lockedItemIds": [],
        },
        "presets": [
            {
                "id": "default",
                "name": "默认封面",
                "templateKey": DEFAULT_TEMPLATE_KEY,
                "pickMode": "random",
                "titleText": "",
                "subtitleText": "",
                "fontKey": "hiragino",
                "titleFontSize": 108,
                "subtitleFontSize": 44,
                "titleAlign": mode_defaults["titleAlign"],
                "overlayStrength": mode_defaults["overlayStrength"],
                "posterCount": mode_defaults["posterCount"],
                "accentTone": mode_defaults["accentTone"],
                "posterRotation": mode_defaults["posterRotation"],
                "titleYOffset": mode_defaults["titleYOffset"],
                "lockedItemIds": [],
            }
        ],
        "backups": {},
        "schedule": {
            "enabled": False,
            "cron": DEFAULT_COVER_STUDIO_CRON,
            "lastRunAt": "",
            "lastStatus": "idle",
            "lastMessage": "未启用自动更新。",
            "lastResultCount": 0,
        },
        "schedules": [],
    }


def normalize_cover_studio_config(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    defaults = default_cover_studio_config()
    draft_source = source.get("draft") if isinstance(source.get("draft"), dict) else {}
    presets_source = source.get("presets") if isinstance(source.get("presets"), list) else defaults["presets"]
    backups_source = source.get("backups") if isinstance(source.get("backups"), dict) else {}
    schedule_source = source.get("schedule") if isinstance(source.get("schedule"), dict) else {}
    schedules_source = source.get("schedules") if isinstance(source.get("schedules"), list) else []
    draft_template_key = _normalize_template_key(draft_source.get("templateKey"))
    draft_mode_defaults = _mode_defaults(draft_template_key)
    draft_view_ids = _normalize_view_ids(draft_source.get("viewIds"), fallback=draft_source.get("viewId"))
    draft = {
        "viewId": draft_view_ids[0] if draft_view_ids else "",
        "viewIds": draft_view_ids,
        "templateKey": draft_template_key,
        "pickMode": "recent" if str(draft_source.get("pickMode") or "").strip().lower() == "recent" else "random",
        "titleText": str(draft_source.get("titleText") or "").strip(),
        "subtitleText": str(draft_source.get("subtitleText") or "").strip(),
        "fontKey": str(draft_source.get("fontKey") or "hiragino").strip() or "hiragino",
        "titleFontSize": _clamp_int(draft_source.get("titleFontSize"), fallback=108, minimum=56, maximum=180),
        "subtitleFontSize": _clamp_int(draft_source.get("subtitleFontSize"), fallback=44, minimum=22, maximum=72),
        "presetName": str(draft_source.get("presetName") or "默认封面").strip() or "默认封面",
        "titleAlign": _normalize_title_align(draft_source.get("titleAlign"), fallback=draft_mode_defaults["titleAlign"]),
        # The cover studio no longer applies a global dark overlay. Keep the
        # legacy field for config compatibility, but normalize it to disabled.
        "overlayStrength": 0,
        "posterCount": _clamp_int(draft_source.get("posterCount"), fallback=draft_mode_defaults["posterCount"], minimum=2, maximum=8),
        "accentTone": _normalize_accent_tone(draft_source.get("accentTone"), fallback=draft_mode_defaults["accentTone"]),
        "posterRotation": _clamp_int(draft_source.get("posterRotation"), fallback=draft_mode_defaults["posterRotation"], minimum=0, maximum=100),
        "titleYOffset": _clamp_int(draft_source.get("titleYOffset"), fallback=draft_mode_defaults["titleYOffset"], minimum=-160, maximum=160),
        "lockedItemIds": [str(item).strip() for item in (draft_source.get("lockedItemIds") or []) if str(item).strip()],
    }
    presets: list[dict[str, Any]] = []
    for index, item in enumerate(presets_source):
        if not isinstance(item, dict):
            continue
        preset_id = str(item.get("id") or f"preset-{index+1}").strip()
        if not preset_id:
            continue
        preset_template_key = _normalize_template_key(item.get("templateKey"))
        preset_mode_defaults = _mode_defaults(preset_template_key)
        presets.append(
            {
                "id": preset_id,
                "name": str(item.get("name") or item.get("presetName") or preset_id).strip() or preset_id,
                "templateKey": preset_template_key,
                "pickMode": "recent" if str(item.get("pickMode") or "").strip().lower() == "recent" else "random",
                "titleText": str(item.get("titleText") or "").strip(),
                "subtitleText": str(item.get("subtitleText") or "").strip(),
                "fontKey": str(item.get("fontKey") or "hiragino").strip() or "hiragino",
                "titleFontSize": _clamp_int(item.get("titleFontSize"), fallback=108, minimum=56, maximum=180),
                "subtitleFontSize": _clamp_int(item.get("subtitleFontSize"), fallback=44, minimum=22, maximum=72),
                "titleAlign": _normalize_title_align(item.get("titleAlign"), fallback=preset_mode_defaults["titleAlign"]),
                "overlayStrength": 0,
                "posterCount": _clamp_int(item.get("posterCount"), fallback=preset_mode_defaults["posterCount"], minimum=2, maximum=8),
                "accentTone": _normalize_accent_tone(item.get("accentTone"), fallback=preset_mode_defaults["accentTone"]),
                "posterRotation": _clamp_int(item.get("posterRotation"), fallback=preset_mode_defaults["posterRotation"], minimum=0, maximum=100),
                "titleYOffset": _clamp_int(item.get("titleYOffset"), fallback=preset_mode_defaults["titleYOffset"], minimum=-160, maximum=160),
                "lockedItemIds": [str(value).strip() for value in (item.get("lockedItemIds") or []) if str(value).strip()],
            }
        )
    if not presets:
        presets = defaults["presets"]
    backups: dict[str, Any] = {}
    for view_id, row in backups_source.items():
        if not isinstance(row, dict):
            continue
        safe_view_id = str(view_id or "").strip()
        if not safe_view_id:
            continue
        backups[safe_view_id] = {
            "primary": _normalize_backup_meta(row.get("primary")),
            "thumb": _normalize_backup_meta(row.get("thumb")),
            "appliedAt": str(row.get("appliedAt") or "").strip(),
        }
    current_preset_id = str(source.get("currentPresetId") or presets[0]["id"]).strip() or presets[0]["id"]
    schedule = normalize_cover_studio_schedule(schedule_source)
    schedules = _normalize_cover_studio_schedules(schedules_source, draft=draft)
    # Upgrade the short-lived global scheduler to independent library plans.
    if not schedules and schedule["enabled"]:
        schedules = _normalize_cover_studio_schedules(
            [{"viewId": view_id, "cron": schedule["cron"], "enabled": True, "template": draft} for view_id in draft_view_ids],
            draft=draft,
        )
    return {
        "currentPresetId": current_preset_id,
        "lastViewId": str(source.get("lastViewId") or draft.get("viewId") or "").strip(),
        "draft": draft,
        "presets": presets,
        "backups": backups,
        "schedule": schedule,
        "schedules": schedules,
    }


def normalize_cover_studio_schedule(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    cron = str(source.get("cron") or DEFAULT_COVER_STUDIO_CRON).strip()
    if not is_valid_cover_studio_cron(cron):
        cron = DEFAULT_COVER_STUDIO_CRON
    status = str(source.get("lastStatus") or "idle").strip().lower()
    if status not in {"idle", "success", "error", "running"}:
        status = "idle"
    try:
        result_count = max(0, min(30, int(source.get("lastResultCount") or 0)))
    except (TypeError, ValueError):
        result_count = 0
    return {
        "enabled": bool(source.get("enabled")),
        "cron": cron,
        "lastRunAt": str(source.get("lastRunAt") or "").strip(),
        "lastStatus": status,
        "lastMessage": str(source.get("lastMessage") or "未启用自动更新。").strip(),
        "lastResultCount": result_count,
    }


def _normalize_cover_studio_schedules(raw: list[Any], *, draft: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_view_ids: set[str] = set()
    for index, value in enumerate(raw[:30]):
        source = value if isinstance(value, dict) else {}
        view_id = str(source.get("viewId") or "").strip()
        if not view_id or view_id in seen_view_ids:
            continue
        seen_view_ids.add(view_id)
        schedule_id = str(source.get("id") or f"view-{view_id}").strip() or f"view-{view_id}"
        cron = str(source.get("cron") or DEFAULT_COVER_STUDIO_CRON).strip()
        if not is_valid_cover_studio_cron(cron):
            cron = DEFAULT_COVER_STUDIO_CRON
        status = str(source.get("lastStatus") or "idle").strip().lower()
        if status not in {"idle", "initialized", "no_change", "success", "error", "running"}:
            status = "idle"
        fingerprint_source = source.get("fingerprint") if isinstance(source.get("fingerprint"), dict) else {}
        try:
            item_count = max(0, int(fingerprint_source.get("itemCount") or 0))
        except (TypeError, ValueError):
            item_count = 0
        rows.append(
            {
                "id": schedule_id,
                "viewId": view_id,
                "viewName": str(source.get("viewName") or "").strip(),
                "enabled": bool(source.get("enabled")),
                "cron": cron,
                "template": _normalize_cover_studio_schedule_template(source.get("template"), fallback=draft),
                "fingerprint": {
                    "itemCount": item_count,
                    "latestItemId": str(fingerprint_source.get("latestItemId") or "").strip(),
                    "latestCreatedAt": str(fingerprint_source.get("latestCreatedAt") or "").strip(),
                },
                "initializedAt": str(source.get("initializedAt") or "").strip(),
                "lastCheckedAt": str(source.get("lastCheckedAt") or "").strip(),
                "lastUpdatedAt": str(source.get("lastUpdatedAt") or "").strip(),
                "lastStatus": status,
                "lastMessage": str(source.get("lastMessage") or "尚未检查。").strip(),
            }
        )
    return rows


def _normalize_cover_studio_schedule_template(raw: Any, *, fallback: dict[str, Any]) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else fallback
    template_key = _normalize_template_key(source.get("templateKey"))
    mode_defaults = _mode_defaults(template_key)
    return {
        "templateKey": template_key,
        "pickMode": "recent" if str(source.get("pickMode") or "").strip().lower() == "recent" else "random",
        "titleText": str(source.get("titleText") or "").strip(),
        "subtitleText": str(source.get("subtitleText") or "").strip(),
        "fontKey": str(source.get("fontKey") or "hiragino").strip() or "hiragino",
        "titleFontSize": _clamp_int(source.get("titleFontSize"), fallback=108, minimum=56, maximum=180),
        "subtitleFontSize": _clamp_int(source.get("subtitleFontSize"), fallback=44, minimum=22, maximum=72),
        "titleAlign": _normalize_title_align(source.get("titleAlign"), fallback=mode_defaults["titleAlign"]),
        "posterCount": _clamp_int(source.get("posterCount"), fallback=mode_defaults["posterCount"], minimum=2, maximum=8),
        "accentTone": _normalize_accent_tone(source.get("accentTone"), fallback=mode_defaults["accentTone"]),
        "posterRotation": _clamp_int(source.get("posterRotation"), fallback=mode_defaults["posterRotation"], minimum=0, maximum=100),
        "titleYOffset": _clamp_int(source.get("titleYOffset"), fallback=mode_defaults["titleYOffset"], minimum=-160, maximum=160),
    }


def is_valid_cover_studio_cron(expression: str) -> bool:
    fields = str(expression or "").strip().split()
    if len(fields) != 5:
        return False
    limits = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 6))
    return all(_is_valid_cron_field(field, minimum, maximum) for field, (minimum, maximum) in zip(fields, limits))


def _is_valid_cron_field(field: str, minimum: int, maximum: int) -> bool:
    for part in str(field or "").split(","):
        chunk = part.strip()
        if not chunk:
            return False
        base, separator, step_text = chunk.partition("/")
        if separator:
            try:
                step = int(step_text)
            except (TypeError, ValueError):
                return False
            if step <= 0 or step > maximum - minimum + 1:
                return False
        if base == "*":
            continue
        if "-" in base:
            start_text, end_text, *extra = base.split("-")
            if extra:
                return False
            try:
                start, end = int(start_text), int(end_text)
            except (TypeError, ValueError):
                return False
            if not (minimum <= start <= end <= maximum):
                return False
            continue
        try:
            value = int(base)
        except (TypeError, ValueError):
            return False
        if not minimum <= value <= maximum:
            return False
    return True


def available_cover_fonts() -> list[dict[str, str]]:
    candidates = [
        {"key": "hiragino", "label": "苹方黑体", "path": "/System/Library/Fonts/Hiragino Sans GB.ttc"},
        {"key": "heiti", "label": "华文黑体", "path": "/System/Library/Fonts/STHeiti Medium.ttc"},
        {"key": "noteworthy", "label": "手写风", "path": "/System/Library/Fonts/Noteworthy.ttc"},
        {"key": "avenir", "label": "Avenir", "path": "/System/Library/Fonts/Avenir.ttc"},
    ]
    rows: list[dict[str, str]] = []
    seen_keys: set[str] = set()
    for item in candidates:
        if os.path.exists(item["path"]):
            rows.append(dict(item))
            seen_keys.add(str(item["key"]).strip())
    for item in _discover_custom_cover_fonts():
        safe_key = str(item.get("key") or "").strip()
        safe_path = str(item.get("path") or "").strip()
        if not safe_key or not safe_path or safe_key in seen_keys or not os.path.exists(safe_path):
            continue
        rows.append(dict(item))
        seen_keys.add(safe_key)
    if not rows:
        rows.append({"key": "default", "label": "默认字体", "path": ""})
    return rows


@dataclass
class CoverStudioPreview:
    token: str
    primary_image_path: pathlib.Path
    primary_image_data_url: str
    primary_width: int
    primary_height: int
    template_key: str
    selected_items: list[dict[str, Any]]


class EmbyCoverService:
    def __init__(self, *, base_url: str, api_key: str, client_name: str = "VistaMirror Cover Studio"):
        self.base_url = str(base_url or "").rstrip("/")
        self.api_key = str(api_key or "").strip()
        self.client_name = str(client_name or "VistaMirror Cover Studio").strip()
        self.ssl_ctx = ssl._create_unverified_context()

    def fetch_user_views(self) -> list[dict[str, Any]]:
        user_views: list[dict[str, Any]] = []
        virtual_views: list[dict[str, Any]] = []
        try:
            rows = self._request_json("/UserViews")
            user_views = self._normalize_user_views(rows)
        except Exception:
            pass
        try:
            fallback = self._request_json("/Library/VirtualFolders")
            virtual_views = self._normalize_virtual_folder_views(fallback)
        except Exception:
            pass
        merged = self._merge_cover_views(user_views, virtual_views)
        if merged:
            return merged
        return user_views or virtual_views

    def fetch_view_items(self, *, view_id: str, pick_mode: str = "random", limit: int = 18) -> list[dict[str, Any]]:
        params = {
            "ParentId": str(view_id or "").strip(),
            "Recursive": "true",
            "IncludeItemTypes": "Movie,Series",
            "Fields": "Name,Type,DateCreated,ImageTags,BackdropImageTags,PrimaryImageItemId,BackdropImageItemId,SeriesId,ParentId,ProductionYear,PremiereDate",
            "Limit": str(max(8, min(limit, 60))),
            "SortBy": "DateCreated",
            "SortOrder": "Descending",
            "ImageTypeLimit": "1",
        }
        query = urllib.parse.urlencode(params)
        payload = self._request_json(f"/Items?{query}")
        rows = payload.get("Items") if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            return []
        items: list[dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            image_tags = item.get("ImageTags") if isinstance(item.get("ImageTags"), dict) else {}
            backdrop_tags = item.get("BackdropImageTags") if isinstance(item.get("BackdropImageTags"), list) else []
            backdrop_tag = str(backdrop_tags[0] or "").strip() if backdrop_tags else ""
            image_item_id = str(item.get("PrimaryImageItemId") or item.get("Id") or "").strip()
            if not image_item_id:
                continue
            if not str(image_tags.get("Primary") or "").strip() and not str(item.get("Id") or "").strip():
                continue
            items.append(
                {
                    "id": str(item.get("Id") or "").strip(),
                    "imageItemId": image_item_id,
                    "primaryTag": str(image_tags.get("Primary") or "").strip(),
                    "backdropImageItemId": str(item.get("BackdropImageItemId") or item.get("Id") or "").strip(),
                    "backdropTag": backdrop_tag,
                    "name": str(item.get("Name") or "").strip(),
                    "type": str(item.get("Type") or "").strip(),
                    "dateCreated": str(item.get("DateCreated") or "").strip(),
                    "productionYear": int(item.get("ProductionYear") or 0),
                    "premiereDate": str(item.get("PremiereDate") or "").strip(),
                }
            )
        if pick_mode == "random" and len(items) > 1:
            pool = list(items)
            random.shuffle(pool)
            items = pool
        return items[: max(4, min(limit, 12))]

    def fetch_view_fingerprint(self, *, view_id: str) -> dict[str, Any]:
        """Return a compact library marker so scheduled refreshes run only on change."""
        params = {
            "ParentId": str(view_id or "").strip(),
            "Recursive": "true",
            "IncludeItemTypes": "Movie,Series,Episode",
            "Fields": "DateCreated",
            "Limit": "1",
            "SortBy": "DateCreated",
            "SortOrder": "Descending",
        }
        payload = self._request_json(f"/Items?{urllib.parse.urlencode(params)}")
        rows = payload.get("Items") if isinstance(payload, dict) else []
        latest = rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else {}
        try:
            item_count = max(0, int(payload.get("TotalRecordCount") or 0)) if isinstance(payload, dict) else 0
        except (TypeError, ValueError):
            item_count = 0
        return {
            "itemCount": item_count,
            "latestItemId": str(latest.get("Id") or "").strip(),
            "latestCreatedAt": str(latest.get("DateCreated") or "").strip(),
        }

    def fetch_primary_image_bytes(self, *, item_id: str, image_tag: str = "", max_width: int = 700) -> bytes:
        params = {"maxWidth": str(max(320, min(int(max_width or 700), 2000))), "quality": "92"}
        if str(image_tag or "").strip():
            params["tag"] = str(image_tag).strip()
        query = urllib.parse.urlencode(params)
        return self._request_bytes(f"/Items/{urllib.parse.quote(str(item_id), safe='')}/Images/Primary?{query}")

    def fetch_backdrop_image_bytes(self, *, item_id: str, image_tag: str = "", max_width: int = 1800) -> bytes:
        """Read an item's horizontal Emby backdrop for cinematic cover templates."""
        params = {
            "maxWidth": str(max(640, min(int(max_width or 1800), 2400))),
            "quality": "94",
            "Index": "0",
        }
        if str(image_tag or "").strip():
            params["tag"] = str(image_tag).strip()
        query = urllib.parse.urlencode(params)
        safe_item_id = urllib.parse.quote(str(item_id or "").strip(), safe="")
        return self._request_bytes(f"/Items/{safe_item_id}/Images/Backdrop?{query}")

    def fetch_view_image_bytes(self, *, view_id: str, image_type: str) -> bytes | None:
        safe_view_id = str(view_id or "").strip()
        safe_type = str(image_type or "").strip()
        if not safe_view_id or not safe_type:
            return None
        try:
            return self._request_bytes(f"/Items/{urllib.parse.quote(safe_view_id, safe='')}/Images/{urllib.parse.quote(safe_type, safe='')}")
        except urllib.error.HTTPError as err:
            if err.code in {404, 400}:
                return None
            raise

    def upload_view_image(self, *, view_id: str, image_type: str, image_bytes: bytes, content_type: str = "image/png") -> None:
        safe_view_id = urllib.parse.quote(str(view_id or "").strip(), safe="")
        safe_type = urllib.parse.quote(str(image_type or "").strip(), safe="")
        payload = base64.b64encode(image_bytes)
        candidates = [
            ("POST", f"/Items/{safe_view_id}/Images/{safe_type}?Index=0"),
            ("POST", f"/Items/{safe_view_id}/Images/{safe_type}"),
        ]
        last_error: Exception | None = None
        for method, path in candidates:
            try:
                # Emby's own web client uploads item images as base64 text, not raw binary bytes.
                self._request_raw(path, method=method, body=payload, headers={"Content-Type": content_type})
                return
            except urllib.error.HTTPError as err:
                last_error = err
                if err.code not in {400, 404, 405, 415}:
                    raise
            except Exception as err:  # pragma: no cover
                last_error = err
        if last_error is not None:
            raise RuntimeError(f"上传 Emby 视图 {image_type} 失败：{last_error}")
        raise RuntimeError(f"上传 Emby 视图 {image_type} 失败")

    def delete_view_image(self, *, view_id: str, image_type: str) -> bool:
        safe_view_id = urllib.parse.quote(str(view_id or "").strip(), safe="")
        safe_type = urllib.parse.quote(str(image_type or "").strip(), safe="")
        if not safe_view_id or not safe_type:
            return False
        try:
            self._request_raw(f"/Items/{safe_view_id}/Images/{safe_type}", method="DELETE")
            return True
        except urllib.error.HTTPError as err:
            if err.code in {400, 404, 405}:
                return False
            raise

    def _request_json(self, path: str) -> dict[str, Any] | list[Any] | None:
        content = self._request_raw(path, method="GET")
        if not content:
            return None
        return json.loads(content.decode("utf-8"))

    def _request_bytes(self, path: str) -> bytes:
        content = self._request_raw(path, method="GET")
        return content or b""

    def _request_raw(
        self,
        path: str,
        *,
        method: str = "GET",
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> bytes:
        request_headers = {
            "X-Emby-Token": self.api_key,
            "X-Emby-Client": self.client_name,
        }
        if headers:
            request_headers.update({str(key): str(value) for key, value in headers.items() if key and value is not None})
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            method=method,
            headers=request_headers,
        )
        with urllib.request.urlopen(request, context=self.ssl_ctx, timeout=45) as response:
            return response.read()

    @staticmethod
    def _normalize_user_views(source: Any) -> list[dict[str, Any]]:
        rows = source.get("Items") if isinstance(source, dict) else source
        if not isinstance(rows, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            item_id = str(item.get("Id") or "").strip()
            name = str(item.get("Name") or "").strip()
            if not item_id or not name:
                continue
            normalized.append(
                {
                    "id": item_id,
                    "browseId": item_id,
                    "uploadTargetId": item_id,
                    "userViewId": item_id,
                    "name": name,
                    "collectionType": str(item.get("CollectionType") or "").strip().lower(),
                    "type": str(item.get("Type") or "").strip(),
                    "imageTags": item.get("ImageTags") if isinstance(item.get("ImageTags"), dict) else {},
                    "backdropImageTags": item.get("BackdropImageTags") if isinstance(item.get("BackdropImageTags"), list) else [],
                    "recursiveItemCount": int(item.get("RecursiveItemCount") or 0),
                    "childCount": int(item.get("ChildCount") or 0),
                    "itemCount": int(item.get("ItemCount") or 0),
                }
            )
        return normalized

    @staticmethod
    def _normalize_virtual_folder_views(source: Any) -> list[dict[str, Any]]:
        rows = source.get("Items") if isinstance(source, dict) else source
        if not isinstance(rows, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            item_id = str(item.get("ItemId") or item.get("Id") or "").strip()
            name = str(item.get("Name") or "").strip()
            if not item_id or not name:
                continue
            normalized.append(
                {
                    "id": item_id,
                    "browseId": item_id,
                    "uploadTargetId": item_id,
                    "virtualFolderId": item_id,
                    "name": name,
                    "collectionType": str(item.get("CollectionType") or "").strip().lower(),
                    "type": str(item.get("Type") or "").strip(),
                    "imageTags": item.get("ImageTags") if isinstance(item.get("ImageTags"), dict) else {},
                    "backdropImageTags": item.get("BackdropImageTags") if isinstance(item.get("BackdropImageTags"), list) else [],
                    "recursiveItemCount": int(item.get("RecursiveItemCount") or 0),
                    "childCount": int(item.get("ChildCount") or 0),
                    "itemCount": int(item.get("ItemCount") or 0),
                }
            )
        return normalized

    @staticmethod
    def _merge_cover_views(user_views: list[dict[str, Any]], virtual_views: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not user_views:
            return list(virtual_views)
        if not virtual_views:
            return list(user_views)

        virtual_index: dict[tuple[str, str], dict[str, Any]] = {}
        virtual_name_index: dict[str, dict[str, Any]] = {}
        for row in virtual_views:
            key = EmbyCoverService._view_match_key(str(row.get("name") or ""), str(row.get("collectionType") or ""))
            if key[0] and key not in virtual_index:
                virtual_index[key] = row
            name_key = key[0]
            if name_key and name_key not in virtual_name_index:
                virtual_name_index[name_key] = row

        merged: list[dict[str, Any]] = []
        matched_virtual_ids: set[str] = set()
        for user_row in user_views:
            key = EmbyCoverService._view_match_key(str(user_row.get("name") or ""), str(user_row.get("collectionType") or ""))
            virtual_row = virtual_index.get(key)
            if virtual_row is None and key[0]:
                virtual_row = virtual_name_index.get(key[0])
            if not isinstance(virtual_row, dict):
                merged.append(user_row)
                continue
            matched_virtual_ids.add(str(virtual_row.get("id") or "").strip())
            merged_row = dict(user_row)
            merged_row["browseId"] = str(user_row.get("browseId") or user_row.get("id") or "").strip()
            merged_row["uploadTargetId"] = str(virtual_row.get("uploadTargetId") or virtual_row.get("id") or "").strip()
            merged_row["userViewId"] = str(user_row.get("userViewId") or user_row.get("id") or "").strip()
            merged_row["virtualFolderId"] = str(virtual_row.get("virtualFolderId") or virtual_row.get("id") or "").strip()
            merged_row["source"] = "merged"
            if not merged_row.get("imageTags") and isinstance(virtual_row.get("imageTags"), dict):
                merged_row["imageTags"] = virtual_row.get("imageTags")
            if not merged_row.get("backdropImageTags") and isinstance(virtual_row.get("backdropImageTags"), list):
                merged_row["backdropImageTags"] = virtual_row.get("backdropImageTags")
            for field in ("recursiveItemCount", "childCount", "itemCount"):
                merged_row[field] = max(int(user_row.get(field) or 0), int(virtual_row.get(field) or 0))
            merged.append(merged_row)

        for virtual_row in virtual_views:
            virtual_id = str(virtual_row.get("id") or "").strip()
            if virtual_id and virtual_id in matched_virtual_ids:
                continue
            merged.append(virtual_row)
        return merged

    @staticmethod
    def _view_match_key(name: str, collection_type: str) -> tuple[str, str]:
        safe_name = "".join(char for char in str(name or "").strip().lower() if char.isalnum())
        safe_collection_type = "".join(char for char in str(collection_type or "").strip().lower() if char.isalnum())
        return safe_name, safe_collection_type


class CoverStudioService:
    def __init__(self, *, data_dir: pathlib.Path):
        self.root = pathlib.Path(data_dir) / "cover_studio"
        self.preview_dir = self.root / "previews"
        self.backup_dir = self.root / "backups"
        self.generated_dir = self.root / "generated"
        self.root.mkdir(parents=True, exist_ok=True)
        self.preview_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.generated_dir.mkdir(parents=True, exist_ok=True)

    def list_fonts(self) -> list[dict[str, str]]:
        return available_cover_fonts()

    def list_modes(self) -> list[dict[str, Any]]:
        return cover_studio_modes()

    def list_accent_tones(self) -> list[dict[str, str]]:
        return cover_studio_accent_tones()

    def list_title_align_options(self) -> list[dict[str, str]]:
        return cover_studio_title_align_options()

    def build_view_status(self, *, view_id: str, config: dict[str, Any]) -> dict[str, Any]:
        backups = config.get("backups") if isinstance(config.get("backups"), dict) else {}
        row = backups.get(str(view_id or "").strip()) if isinstance(backups.get(str(view_id or "").strip()), dict) else {}
        return {
            "hasBackupPrimary": bool((row.get("primary") or {}).get("path")) if isinstance(row, dict) else False,
            "appliedAt": str((row.get("appliedAt") or "") if isinstance(row, dict) else "").strip(),
        }

    def generate_preview(
        self,
        *,
        view: dict[str, Any],
        items: list[dict[str, Any]],
        template_key: str,
        font_key: str,
        title_text: str,
        subtitle_text: str,
        title_font_size: int,
        subtitle_font_size: int,
        title_align: str,
        overlay_strength: int,
        poster_count: int,
        accent_tone: str,
        poster_rotation: int,
        title_y_offset: int,
        emby_service: EmbyCoverService,
    ) -> CoverStudioPreview:
        if not items:
            raise RuntimeError("该媒体库下暂无可用于生成封面的海报。")
        prepared: list[tuple[dict[str, Any], Image.Image]] = []
        for item in items[:8]:
            try:
                content = emby_service.fetch_primary_image_bytes(
                    item_id=str(item.get("imageItemId") or item.get("id") or "").strip(),
                    image_tag=str(item.get("primaryTag") or "").strip(),
                )
                if not content:
                    continue
                image = Image.open(BytesIO(content)).convert("RGB")
                prepared.append((item, image))
            except Exception:
                continue
        if not prepared:
            raise RuntimeError("未能从 Emby 读取到可用海报，无法生成预览。")
        safe_template_key = _normalize_template_key(template_key)
        hero_image = self._resolve_showcase_hero_image(
            template_key=safe_template_key,
            prepared=prepared,
            emby_service=emby_service,
        )
        safe_title_size = _clamp_int(title_font_size, fallback=108, minimum=56, maximum=180)
        safe_subtitle_size = _clamp_int(subtitle_font_size, fallback=44, minimum=22, maximum=72)
        safe_title_align = _normalize_title_align(title_align, fallback=_mode_defaults(safe_template_key)["titleAlign"])
        # Older saved drafts may still contain a non-zero value. Ignore it so
        # every generated template follows the same no-global-mask rule.
        safe_overlay_strength = 0
        safe_poster_count = min(len(prepared), _clamp_int(poster_count, fallback=_mode_defaults(safe_template_key)["posterCount"], minimum=2, maximum=8))
        safe_accent_tone = _normalize_accent_tone(accent_tone, fallback=_mode_defaults(safe_template_key)["accentTone"])
        safe_poster_rotation = _clamp_int(poster_rotation, fallback=_mode_defaults(safe_template_key)["posterRotation"], minimum=0, maximum=100)
        safe_title_y_offset = _clamp_int(title_y_offset, fallback=_mode_defaults(safe_template_key)["titleYOffset"], minimum=-160, maximum=160)
        mode = _mode_meta(safe_template_key)
        primary_canvas = self._render_mode_cover(
            template_key=safe_template_key,
            images=[image for _, image in prepared][:safe_poster_count],
            hero_image=hero_image,
            title_text=title_text,
            subtitle_text=subtitle_text,
            font_key=font_key,
            title_font_size=safe_title_size,
            subtitle_font_size=safe_subtitle_size,
            title_align=safe_title_align,
            overlay_strength=safe_overlay_strength,
            accent_tone=safe_accent_tone,
            poster_rotation=safe_poster_rotation,
            title_y_offset=safe_title_y_offset,
        )
        token = secrets.token_urlsafe(18)
        primary_target = self.preview_dir / f"{token}_primary.png"
        primary_canvas.save(primary_target, format="PNG", optimize=True)
        primary_payload = base64.b64encode(primary_target.read_bytes()).decode("ascii")
        selected_items = [
            {"id": row.get("id"), "name": row.get("name"), "imageItemId": row.get("imageItemId")}
            for row, _ in prepared[:safe_poster_count]
        ]
        return CoverStudioPreview(
            token=token,
            primary_image_path=primary_target,
            primary_image_data_url=f"data:image/png;base64,{primary_payload}",
            primary_width=primary_canvas.width,
            primary_height=primary_canvas.height,
            template_key=safe_template_key,
            selected_items=selected_items,
        )

    @staticmethod
    def _resolve_showcase_hero_image(
        *,
        template_key: str,
        prepared: list[tuple[dict[str, Any], Image.Image]],
        emby_service: EmbyCoverService,
    ) -> Image.Image:
        """Prefer Emby's backdrop so the hero stays cinematic instead of a blown-up poster."""
        mode = _mode_meta(template_key)
        if str(mode.get("family") or "").strip() != "cinematic_showcase":
            return prepared[0][1]
        fetch_backdrop = getattr(emby_service, "fetch_backdrop_image_bytes", None)
        if callable(fetch_backdrop):
            for item, _poster in prepared:
                item_id = str(item.get("backdropImageItemId") or item.get("id") or "").strip()
                if not item_id:
                    continue
                try:
                    payload = fetch_backdrop(
                        item_id=item_id,
                        image_tag=str(item.get("backdropTag") or "").strip(),
                    )
                    if payload:
                        return Image.open(BytesIO(payload)).convert("RGB")
                except Exception:
                    continue
        return prepared[0][1]

    def backup_and_apply(
        self,
        *,
        config: dict[str, Any],
        view_id: str,
        upload_view_id: str | None = None,
        preview_token: str,
        emby_service: EmbyCoverService,
    ) -> dict[str, Any]:
        primary_preview_path = self.preview_dir / f"{preview_token}_primary.png"
        legacy_preview_path = self.preview_dir / f"{preview_token}.png"
        if primary_preview_path.exists():
            primary_payload = primary_preview_path.read_bytes()
        elif legacy_preview_path.exists():
            primary_payload = legacy_preview_path.read_bytes()
        else:
            raise RuntimeError("预览已失效，请重新生成后再应用。")
        backups = config.setdefault("backups", {})
        safe_view_id = str(view_id or "").strip()
        safe_upload_view_id = str(upload_view_id or safe_view_id).strip() or safe_view_id
        row = backups.get(safe_view_id) if isinstance(backups.get(safe_view_id), dict) else {"primary": {}, "thumb": {}, "appliedAt": ""}
        existing = emby_service.fetch_view_image_bytes(view_id=safe_upload_view_id, image_type="Primary")
        if existing is not None:
            backup_path = self.backup_dir / f"{safe_view_id}_primary.bin"
            backup_path.write_bytes(existing)
            row["primary"] = {
                "path": str(backup_path),
                "contentType": "image/jpeg" if existing[:3] == b"\xff\xd8\xff" else "image/png",
            }
        emby_service.upload_view_image(view_id=safe_upload_view_id, image_type="Primary", image_bytes=primary_payload)
        thumb_removed = False
        thumb_warning = ""
        delete_thumb = getattr(emby_service, "delete_view_image", None)
        if callable(delete_thumb):
            try:
                thumb_removed = bool(delete_thumb(view_id=safe_upload_view_id, image_type="Thumb"))
            except Exception as err:  # Keep the Primary update successful if Thumb is unsupported.
                thumb_warning = str(err)
        row["thumb"] = {}
        generated_primary_path = self.generated_dir / f"{safe_view_id}_{int(time.time())}_primary.png"
        generated_primary_path.write_bytes(primary_payload)
        row["appliedAt"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
        row["uploadTargetId"] = safe_upload_view_id
        backups[safe_view_id] = row
        return {
            "appliedAt": row["appliedAt"],
            "generatedPrimaryPath": str(generated_primary_path),
            "uploadTargetId": safe_upload_view_id,
            "thumbRemoved": thumb_removed,
            "thumbWarning": thumb_warning,
        }

    def restore_backup(
        self,
        *,
        config: dict[str, Any],
        view_id: str,
        upload_view_id: str | None = None,
        emby_service: EmbyCoverService,
    ) -> dict[str, Any]:
        backups = config.get("backups") if isinstance(config.get("backups"), dict) else {}
        row = backups.get(str(view_id or "").strip()) if isinstance(backups.get(str(view_id or "").strip()), dict) else None
        if not row:
            raise RuntimeError("该视图没有可恢复的封面备份。")
        target_view_id = str(upload_view_id or (row.get("uploadTargetId") if isinstance(row, dict) else "") or view_id or "").strip()
        restored = []
        image_type = "Primary"
        meta = row.get("primary") if isinstance(row.get("primary"), dict) else {}
        path_value = str(meta.get("path") or "").strip()
        if path_value:
            backup_path = pathlib.Path(path_value)
            if backup_path.exists():
                emby_service.upload_view_image(
                    view_id=target_view_id,
                    image_type=image_type,
                    image_bytes=backup_path.read_bytes(),
                    content_type=str(meta.get("contentType") or "image/png"),
                )
                restored.append(image_type)
        if not restored:
            primary_meta = row.get("primary") if isinstance(row.get("primary"), dict) else {}
            primary_path_value = str(primary_meta.get("path") or "").strip()
            if primary_path_value:
                backup_path = pathlib.Path(primary_path_value)
                if backup_path.exists():
                    emby_service.upload_view_image(
                        view_id=target_view_id,
                        image_type="Primary",
                        image_bytes=backup_path.read_bytes(),
                        content_type=str(primary_meta.get("contentType") or "image/png"),
                    )
                    restored.append("Primary")
        if not restored:
            raise RuntimeError("备份文件不存在，无法恢复。")
        row["appliedAt"] = ""
        return {"restored": restored}

    def cleanup_previews(self) -> None:
        threshold = time.time() - PREVIEW_TTL_SECONDS
        for path in self.preview_dir.glob("*.png"):
            try:
                if path.stat().st_mtime < threshold:
                    path.unlink()
            except Exception:
                continue

    def _render_mode_cover(
        self,
        *,
        template_key: str,
        images: list[Image.Image],
        hero_image: Image.Image | None,
        title_text: str,
        subtitle_text: str,
        font_key: str,
        title_font_size: int,
        subtitle_font_size: int,
        title_align: str,
        overlay_strength: int,
        accent_tone: str,
        poster_rotation: int,
        title_y_offset: int,
    ) -> Image.Image:
        tone = _tone_palette(accent_tone)
        title_font = self._load_font(font_key, title_font_size)
        subtitle_font = self._load_font(font_key, subtitle_font_size)
        mode = _mode_meta(template_key)
        if str(mode.get("family") or "").strip() == "cinematic_showcase":
            canvas = self._render_cinematic_showcase_cover(
                images=images,
                hero_image=hero_image,
                tone=tone,
                overlay_strength=overlay_strength,
                poster_rotation=poster_rotation,
                variant=str(mode.get("variant") or "banner").strip() or "banner",
            )
        elif template_key == "fan_spread":
            canvas = self._render_fan_spread_cover(images=images, tone=tone, overlay_strength=overlay_strength, poster_rotation=poster_rotation)
        elif template_key == "rotated_stack":
            canvas = self._render_rotated_stack_cover(images=images, tone=tone, overlay_strength=overlay_strength, poster_rotation=poster_rotation)
        elif template_key == "poster_wall":
            canvas = self._render_poster_wall_cover(images=images, tone=tone, overlay_strength=overlay_strength)
        elif template_key == "focus_poster":
            canvas = self._render_focus_poster_cover(images=images, tone=tone, overlay_strength=overlay_strength, poster_rotation=poster_rotation)
        elif template_key == "glory_view":
            canvas = self._render_glory_view_cover(images=images, tone=tone, overlay_strength=overlay_strength)
        else:
            canvas = self._render_stack_cover(images=images, tone=tone, overlay_strength=overlay_strength, poster_rotation=poster_rotation)
        x_bounds = (88, int(canvas.width * 0.62))
        base_y = 472
        if str(mode.get("family") or "").strip() == "cinematic_showcase":
            showcase_meta = _cinematic_showcase_variant(str(mode.get("variant") or "banner").strip() or "banner", thumb=False)
            title_meta = showcase_meta["title"]
            draw = ImageDraw.Draw(canvas)
            self._draw_showcase_title_block(
                draw=draw,
                title_text=title_text.strip() or "未命名媒体库",
                subtitle_text=subtitle_text.strip(),
                title_font=title_font,
                subtitle_font=subtitle_font,
                align=title_align,
                x_bounds=title_meta["x_bounds"],
                base_y=title_meta["base_y"],
                title_y_offset=title_y_offset,
                tone=tone,
                decor=title_meta["decor"],
            )
            return canvas.convert("RGB")
        if template_key == "fan_spread":
            x_bounds = (72, 520)
            base_y = 548
        draw = ImageDraw.Draw(canvas)
        self._draw_title_block(
            draw=draw,
            title_text=title_text.strip() or "未命名媒体库",
            subtitle_text=subtitle_text.strip(),
            title_font=title_font,
            subtitle_font=subtitle_font,
            align=title_align,
            x_bounds=x_bounds,
            base_y=base_y,
            title_y_offset=title_y_offset,
            tone=tone,
        )
        return canvas.convert("RGB")

    def _render_thumb_cover(
        self,
        *,
        template_key: str,
        images: list[Image.Image],
        title_text: str,
        subtitle_text: str,
        font_key: str,
        title_font_size: int,
        subtitle_font_size: int,
        title_align: str,
        overlay_strength: int,
        accent_tone: str,
        poster_rotation: int,
        title_y_offset: int,
        mode: dict[str, Any],
    ) -> Image.Image:
        tone = _tone_palette(accent_tone)
        mode = _mode_meta(template_key)
        if str(mode.get("family") or "").strip() == "cinematic_showcase":
            background = self._render_cinematic_showcase_thumb(
                images=images,
                tone=tone,
                overlay_strength=overlay_strength,
                poster_rotation=poster_rotation,
                variant=str(mode.get("variant") or "banner").strip() or "banner",
            )
        elif template_key == "fan_spread":
            background = self._build_showcase_stage_background(
                size=DEFAULT_THUMB_SIZE,
                tone=tone,
                overlay_strength=overlay_strength,
                title_area_ratio=0.58,
            )
        else:
            background = self._build_background(images[0], DEFAULT_THUMB_SIZE, overlay_strength=overlay_strength, tone=tone, left_weight=0.58)
        title_font = self._load_font(font_key, title_font_size)
        subtitle_font = self._load_font(font_key, subtitle_font_size)
        if str(mode.get("family") or "").strip() == "cinematic_showcase":
            title_meta = _cinematic_showcase_variant(str(mode.get("variant") or "banner").strip() or "banner", thumb=True)["title"]
            draw = ImageDraw.Draw(background)
            self._draw_showcase_title_block(
                draw=draw,
                title_text=title_text.strip() or "未命名媒体库",
                subtitle_text=subtitle_text.strip(),
                title_font=title_font,
                subtitle_font=subtitle_font,
                align=title_align,
                x_bounds=title_meta["x_bounds"],
                base_y=title_meta["base_y"],
                title_y_offset=title_y_offset,
                tone=tone,
                decor=title_meta["decor"],
            )
            return background.convert("RGB")
        if template_key == "poster_wall":
            thumb_specs = [
                ((760, 100), (170, 248)),
                ((952, 100), (170, 248)),
                ((1144, 100), (170, 248)),
                ((856, 360), (170, 248)),
                ((1048, 360), (170, 248)),
            ]
            for idx, (origin, size) in enumerate(thumb_specs[: len(images)]):
                poster = self._create_poster_card(images[idx], size=size, rotation=0, radius=22)
                self._alpha_paste(background, poster, origin)
        elif template_key == "fan_spread":
            poster_specs = [
                ((440, 238), (124, 182), -10.0),
                ((564, 186), (146, 214), -5.5),
                ((706, 124), (180, 264), -2.5),
                ((874, 96), (214, 314), 0.0),
                ((1078, 124), (180, 264), 2.5),
                ((1220, 186), (146, 214), 5.5),
                ((1338, 238), (124, 182), 10.0),
            ]
            spec_count = min(len(images), len(poster_specs))
            spec_start = max(0, (len(poster_specs) - spec_count) // 2)
            visible_specs = poster_specs[spec_start : spec_start + spec_count]
            visible_images = images[:spec_count]
            center_index = len(visible_images) // 2
            ordering = sorted(range(len(visible_images)), key=lambda idx: abs(idx - center_index), reverse=True)
            for idx in ordering:
                image = visible_images[idx]
                origin, size, base_angle = visible_specs[idx]
                card = self._create_showcase_poster_card(
                    image,
                    size=size,
                    rotation=base_angle + (max(0, min(poster_rotation, 100)) - 50) * 0.035,
                    radius=24 if idx == center_index else 20,
                    glow_color=tone["glow"] if idx == center_index else tone["accent"],
                    glow_alpha=124 if idx == center_index else 78,
                )
                self._paste_with_reflection(
                    background,
                    card,
                    origin,
                    opacity=0.2 if idx == center_index else 0.12,
                    blur_radius=12 if idx == center_index else 9,
                    scale=0.62,
                )
        elif template_key == "focus_poster":
            hero = self._create_poster_card(images[0], size=(294, 430), rotation=_rotation_from_strength(poster_rotation, base=-2.0), radius=24)
            self._alpha_paste(background, hero, (870, 128))
            for idx, image in enumerate(images[1:3]):
                card = self._create_poster_card(image, size=(140, 204), rotation=_rotation_from_strength(poster_rotation, base=2.0 + idx), radius=18)
                self._alpha_paste(background, card, (1100 + idx * 86, 400 + idx * 16))
        else:
            hero = self._create_poster_card(images[0], size=(264, 386), rotation=_rotation_from_strength(poster_rotation, base=0), radius=24)
            self._alpha_paste(background, hero, (902, 144))
            if len(images) > 1:
                accent = self._create_poster_card(images[1], size=(158, 232), rotation=_rotation_from_strength(poster_rotation, base=3.0), radius=18)
                self._alpha_paste(background, accent, (1114, 344))
        draw = ImageDraw.Draw(background)
        self._draw_title_block(
            draw=draw,
            title_text=title_text.strip() or "未命名媒体库",
            subtitle_text=subtitle_text.strip(),
            title_font=title_font,
            subtitle_font=subtitle_font,
            align=title_align,
            x_bounds=(72, 474) if template_key == "fan_spread" else (72, 760),
            base_y=458 if template_key == "fan_spread" else 350,
            title_y_offset=title_y_offset,
            tone=tone,
        )
        label_font = self._load_font(font_key, 24)
        label = str(mode.get("label") or "").strip()
        if label and template_key != "fan_spread":
            self._draw_text_shadow(
                draw,
                (74, 74),
                label,
                label_font,
                fill=(tone["accent"][0], tone["accent"][1], tone["accent"][2], 220),
                shadow=(8, 18, 28, 120),
            )
        return background.convert("RGB")

    def _render_cinematic_showcase_cover(
        self,
        *,
        images: list[Image.Image],
        hero_image: Image.Image | None,
        tone: dict[str, tuple[int, int, int]],
        overlay_strength: int,
        poster_rotation: int,
        variant: str,
    ) -> Image.Image:
        meta = _cinematic_showcase_variant(variant, thumb=False)
        hero_source = hero_image or images[min(len(images) - 1, meta.get("hero_image_index", 0))]
        canvas = self._build_cinematic_showcase_background(
            source_image=images[0],
            hero_image=hero_source,
            size=DEFAULT_CANVAS_SIZE,
            tone=tone,
            overlay_strength=overlay_strength,
            variant_meta=meta,
        )
        self._paste_cinematic_posters(
            canvas,
            images=images[: max(2, min(len(images), meta.get("poster_limit", 6)))],
            specs=meta["poster_specs"],
            tone=tone,
            poster_rotation=poster_rotation,
            reflection_opacity=meta["reflection_opacity"],
            reflection_scale=meta["reflection_scale"],
            angle_tune_scale=float(meta.get("angle_tune_scale", 0.035)),
            strict_poster_row=bool(meta.get("strict_poster_row")),
        )
        return canvas

    def _render_cinematic_showcase_thumb(
        self,
        *,
        images: list[Image.Image],
        hero_image: Image.Image | None = None,
        tone: dict[str, tuple[int, int, int]],
        overlay_strength: int,
        poster_rotation: int,
        variant: str,
    ) -> Image.Image:
        meta = _cinematic_showcase_variant(variant, thumb=True)
        hero_source = hero_image or images[min(len(images) - 1, meta.get("hero_image_index", 0))]
        canvas = self._build_cinematic_showcase_background(
            source_image=images[0],
            hero_image=hero_source,
            size=DEFAULT_THUMB_SIZE,
            tone=tone,
            overlay_strength=overlay_strength,
            variant_meta=meta,
        )
        self._paste_cinematic_posters(
            canvas,
            images=images[: max(2, min(len(images), meta.get("poster_limit", 5)))],
            specs=meta["poster_specs"],
            tone=tone,
            poster_rotation=poster_rotation,
            reflection_opacity=meta["reflection_opacity"],
            reflection_scale=meta["reflection_scale"],
            angle_tune_scale=float(meta.get("angle_tune_scale", 0.035)),
            strict_poster_row=bool(meta.get("strict_poster_row")),
        )
        return canvas

    def _render_stack_cover(
        self,
        *,
        images: list[Image.Image],
        tone: dict[str, tuple[int, int, int]],
        overlay_strength: int,
        poster_rotation: int,
    ) -> Image.Image:
        canvas = self._build_background(images[0], DEFAULT_CANVAS_SIZE, overlay_strength=overlay_strength, tone=tone, left_weight=0.62)
        specs = [
            ((1064, 516), (278, 404), 0.0),
            ((1230, 558), (214, 322), 2.4),
            ((1350, 602), (176, 266), 4.0),
            ((1450, 636), (148, 224), 6.0),
        ]
        for idx, (origin, size, base) in enumerate(specs[: len(images)]):
            poster = self._create_poster_card(
                images[idx],
                size=size,
                rotation=_rotation_from_strength(poster_rotation, base=base),
                radius=26,
            )
            self._alpha_paste(canvas, poster, origin)
        return canvas

    def _render_fan_spread_cover(
        self,
        *,
        images: list[Image.Image],
        tone: dict[str, tuple[int, int, int]],
        overlay_strength: int,
        poster_rotation: int,
    ) -> Image.Image:
        canvas = self._build_showcase_stage_background(
            size=DEFAULT_CANVAS_SIZE,
            tone=tone,
            overlay_strength=overlay_strength,
            title_area_ratio=0.32,
        )
        poster_specs = [
            ((320, 234), (174, 256), -10.5),
            ((478, 176), (206, 304), -6.5),
            ((654, 124), (236, 348), -3.0),
            ((850, 76), (280, 414), 0.0),
            ((1088, 124), (236, 348), 3.0),
            ((1278, 176), (206, 304), 6.5),
            ((1430, 234), (174, 256), 10.5),
        ]
        spec_count = min(len(images), len(poster_specs))
        spec_start = max(0, (len(poster_specs) - spec_count) // 2)
        visible_specs = poster_specs[spec_start : spec_start + spec_count]
        visible_images = images[:spec_count]
        center_index = len(visible_images) // 2
        ordering = sorted(range(len(visible_images)), key=lambda idx: abs(idx - center_index), reverse=True)
        angle_tune = (max(0, min(poster_rotation, 100)) - 50) * 0.06
        for idx in ordering:
            image = visible_images[idx]
            origin, size, base_angle = visible_specs[idx]
            card = self._create_showcase_poster_card(
                image,
                size=size,
                rotation=base_angle + angle_tune,
                radius=28 if idx == center_index else 24,
                glow_color=tone["glow"] if idx == center_index else tone["accent"],
                glow_alpha=132 if idx == center_index else 82,
            )
            self._paste_with_reflection(
                canvas,
                card,
                origin,
                opacity=0.22 if idx == center_index else 0.13,
                blur_radius=13 if idx == center_index else 10,
                scale=0.68,
            )
        return canvas

    def _render_rotated_stack_cover(
        self,
        *,
        images: list[Image.Image],
        tone: dict[str, tuple[int, int, int]],
        overlay_strength: int,
        poster_rotation: int,
    ) -> Image.Image:
        canvas = self._build_background(images[0], DEFAULT_CANVAS_SIZE, overlay_strength=overlay_strength, tone=tone, left_weight=0.54)
        center_x = 1218
        center_y = 472
        radius_x = 120
        radius_y = 92
        rotation_factor = max(8.0, poster_rotation * 0.18)
        for idx, image in enumerate(images):
            offset_x = int(math.cos(idx / max(1, len(images)) * math.pi * 1.22) * radius_x)
            offset_y = int(math.sin(idx / max(1, len(images)) * math.pi * 1.22) * radius_y)
            angle = (idx - (len(images) - 1) / 2) * rotation_factor * 0.32
            poster = self._create_poster_card(image, size=(230, 336), rotation=angle, radius=24)
            self._alpha_paste(canvas, poster, (center_x + offset_x, center_y + offset_y))
        return canvas

    def _render_poster_wall_cover(
        self,
        *,
        images: list[Image.Image],
        tone: dict[str, tuple[int, int, int]],
        overlay_strength: int,
    ) -> Image.Image:
        canvas = self._build_background(images[0], DEFAULT_CANVAS_SIZE, overlay_strength=overlay_strength, tone=tone, left_weight=0.48)
        card_size = (166, 244)
        start_x = 978
        start_y = 150
        gap_x = 28
        gap_y = 28
        columns = 3
        for idx, image in enumerate(images[:6]):
            row = idx // columns
            col = idx % columns
            poster = self._create_poster_card(image, size=card_size, rotation=0, radius=22)
            x = start_x + col * (card_size[0] + gap_x)
            y = start_y + row * (card_size[1] + gap_y)
            self._alpha_paste(canvas, poster, (x, y))
        return canvas

    def _render_focus_poster_cover(
        self,
        *,
        images: list[Image.Image],
        tone: dict[str, tuple[int, int, int]],
        overlay_strength: int,
        poster_rotation: int,
    ) -> Image.Image:
        canvas = self._build_background(images[0], DEFAULT_CANVAS_SIZE, overlay_strength=overlay_strength, tone=tone, left_weight=0.68)
        hero = self._create_poster_card(
            images[0],
            size=(360, 526),
            rotation=_rotation_from_strength(poster_rotation, base=-2.2),
            radius=28,
        )
        self._alpha_paste(canvas, hero, (1036, 190))
        for idx, image in enumerate(images[1:4]):
            poster = self._create_poster_card(
                image,
                size=(154, 228),
                rotation=_rotation_from_strength(poster_rotation, base=3.2 + idx),
                radius=18,
            )
            self._alpha_paste(canvas, poster, (1282 + idx * 84, 516 + idx * 10))
        return canvas

    def _render_glory_view_cover(
        self,
        *,
        images: list[Image.Image],
        tone: dict[str, tuple[int, int, int]],
        overlay_strength: int,
    ) -> Image.Image:
        canvas = self._build_background(images[0], DEFAULT_CANVAS_SIZE, overlay_strength=overlay_strength, tone=tone, left_weight=0.72)
        halo = Image.new("RGBA", DEFAULT_CANVAS_SIZE, (0, 0, 0, 0))
        halo_draw = ImageDraw.Draw(halo)
        halo_draw.ellipse((890, 120, 1640, 860), fill=(tone["accent"][0], tone["accent"][1], tone["accent"][2], 46))
        halo = halo.filter(ImageFilter.GaussianBlur(radius=60))
        canvas = Image.alpha_composite(canvas, halo)
        hero = self._create_poster_card(images[0], size=(294, 430), rotation=-2.0, radius=26)
        self._alpha_paste(canvas, hero, (1090, 246))
        for idx, image in enumerate(images[1:4]):
            poster = self._create_poster_card(image, size=(156, 232), rotation=0.8 * idx, radius=18)
            self._alpha_paste(canvas, poster, (1318 + idx * 58, 438 + idx * 16))
        return canvas

    def _build_cinematic_showcase_background(
        self,
        *,
        source_image: Image.Image,
        hero_image: Image.Image,
        size: tuple[int, int],
        tone: dict[str, tuple[int, int, int]],
        overlay_strength: int,
        variant_meta: dict[str, Any],
    ) -> Image.Image:
        if str(variant_meta.get("layout_style") or "").strip() == "streaming_banner":
            return self._build_streaming_banner_background(
                hero_image=hero_image,
                size=size,
                tone=tone,
                variant_meta=variant_meta,
            )
        width, height = size
        background = ImageOps_fit(source_image, size).filter(ImageFilter.GaussianBlur(radius=12))
        overlay = Image.new("RGBA", size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        darkness = min(228, 88 + int(max(0, min(overlay_strength, 100)) * 1.15))
        left_panel = int(width * variant_meta["left_panel_ratio"])
        draw.rectangle((0, 0, width, height), fill=(4, 8, 14, 86 + darkness // 3))
        draw.rectangle((0, 0, left_panel, height), fill=(3, 6, 12, darkness))
        draw.rectangle((0, int(height * 0.64), width, height), fill=(5, 6, 10, min(220, darkness + 24)))
        accent = tone["accent"]
        glow = tone["glow"]
        draw.ellipse(
            (
                int(width * 0.54),
                -int(height * 0.08),
                int(width * 1.02),
                int(height * 0.7),
            ),
            fill=(glow[0], glow[1], glow[2], variant_meta["glow_alpha"]),
        )
        draw.ellipse(
            (
                -int(width * 0.14),
                int(height * 0.52),
                int(width * 0.36),
                int(height * 1.06),
            ),
            fill=(accent[0], accent[1], accent[2], variant_meta["floor_glow_alpha"]),
        )
        if overlay_strength <= 0:
            canvas = background.convert("RGBA")
        else:
            overlay = overlay.filter(ImageFilter.GaussianBlur(radius=18))
            canvas = Image.alpha_composite(background.convert("RGBA"), overlay)

        hero_box = variant_meta["hero_box"]
        hero_layer = Image.new("RGBA", size, (0, 0, 0, 0))
        hero_crop = ImageOps_fit(hero_image, (hero_box[2], hero_box[3]))
        hero_panel = Image.new("RGBA", (hero_box[2], hero_box[3]), (0, 0, 0, 0))
        hero_panel.paste(hero_crop, (0, 0))
        hero_mask = Image.new("L", (hero_box[2], hero_box[3]), 0)
        hero_draw = ImageDraw.Draw(hero_mask)
        hero_draw.rounded_rectangle((0, 0, hero_box[2], hero_box[3]), radius=variant_meta["hero_radius"], fill=255)
        hero_mask = hero_mask.filter(ImageFilter.GaussianBlur(radius=variant_meta["hero_mask_blur"]))
        hero_panel.putalpha(hero_mask.point(lambda value: int(value * variant_meta["hero_opacity"])))
        hero_layer.alpha_composite(hero_panel, (hero_box[0], hero_box[1]))
        hero_glow = Image.new("RGBA", size, (0, 0, 0, 0))
        glow_draw = ImageDraw.Draw(hero_glow)
        glow_draw.ellipse(
            (
                hero_box[0] - 90,
                hero_box[1] - 80,
                hero_box[0] + hero_box[2] + 110,
                hero_box[1] + hero_box[3] + 140,
            ),
            fill=(accent[0], accent[1], accent[2], variant_meta["hero_ring_alpha"]),
        )
        hero_glow = hero_glow.filter(ImageFilter.GaussianBlur(radius=42))
        canvas = Image.alpha_composite(canvas, hero_glow)
        canvas = Image.alpha_composite(canvas, hero_layer)

        return canvas

    def _build_streaming_banner_background(
        self,
        *,
        hero_image: Image.Image,
        size: tuple[int, int],
        tone: dict[str, tuple[int, int, int]],
        variant_meta: dict[str, Any],
    ) -> Image.Image:
        """Build the unified dark streaming panel used by the horizontal showcase."""
        width, height = size
        inset = int(variant_meta.get("frame_inset", 36))
        radius = int(variant_meta.get("frame_radius", 40))
        inner_box = (inset, inset, width - inset, height - inset)
        inner_size = (max(1, inner_box[2] - inner_box[0]), max(1, inner_box[3] - inner_box[1]))

        canvas = Image.new("RGBA", size, (4, 8, 14, 255))
        ambient = Image.new("RGBA", size, (0, 0, 0, 0))
        ambient_draw = ImageDraw.Draw(ambient)
        ambient_draw.ellipse(
            (int(width * 0.48), -int(height * 0.18), int(width * 1.08), int(height * 0.62)),
            fill=(tone["glow"][0], tone["glow"][1], tone["glow"][2], 28),
        )
        ambient = ambient.filter(ImageFilter.GaussianBlur(radius=max(20, inset)))
        canvas = Image.alpha_composite(canvas, ambient)

        hero = ImageOps_fit(hero_image, inner_size).convert("RGBA")
        hero_mask = Image.new("L", inner_size, 0)
        ImageDraw.Draw(hero_mask).rounded_rectangle(
            (0, 0, inner_size[0], inner_size[1]),
            radius=radius,
            fill=255,
        )
        hero.putalpha(hero_mask)
        canvas.alpha_composite(hero, (inner_box[0], inner_box[1]))

        # A permanent left-side falloff keeps titles legible without a glass tray.
        title_ratio = max(0.28, min(float(variant_meta.get("left_panel_ratio", 0.36)), 0.50))
        gradient = Image.new("RGBA", inner_size, (0, 0, 0, 0))
        gradient_pixels = gradient.load()
        edge = max(1, int(inner_size[0] * title_ratio))
        fade_end = min(inner_size[0] - 1, int(inner_size[0] * (title_ratio + 0.18)))
        for x in range(inner_size[0]):
            if x <= edge:
                alpha = 224
            elif x >= fade_end:
                alpha = 20
            else:
                progress = (x - edge) / max(1, fade_end - edge)
                alpha = int(224 - progress * 204)
            for y in range(inner_size[1]):
                gradient_pixels[x, y] = (2, 6, 12, alpha)
        gradient = gradient.filter(ImageFilter.GaussianBlur(radius=12))
        canvas.alpha_composite(gradient, (inner_box[0], inner_box[1]))

        bottom_shade = Image.new("RGBA", inner_size, (0, 0, 0, 0))
        bottom_draw = ImageDraw.Draw(bottom_shade)
        bottom_draw.rectangle(
            (0, int(inner_size[1] * 0.60), inner_size[0], inner_size[1]),
            fill=(1, 4, 10, 76),
        )
        bottom_shade = bottom_shade.filter(ImageFilter.GaussianBlur(radius=18))
        canvas.alpha_composite(bottom_shade, (inner_box[0], inner_box[1]))

        frame = Image.new("RGBA", size, (0, 0, 0, 0))
        frame_draw = ImageDraw.Draw(frame)
        frame_draw.rounded_rectangle(
            inner_box,
            radius=radius,
            outline=(196, 216, 240, int(variant_meta.get("frame_outline_alpha", 108))),
            width=max(1, int(inset * 0.06)),
        )
        canvas = Image.alpha_composite(canvas, frame)
        return canvas

    def _paste_cinematic_posters(
        self,
        canvas: Image.Image,
        *,
        images: list[Image.Image],
        specs: list[dict[str, Any]],
        tone: dict[str, tuple[int, int, int]],
        poster_rotation: int,
        reflection_opacity: float,
        reflection_scale: float,
        angle_tune_scale: float,
        strict_poster_row: bool = False,
    ) -> None:
        spec_count = min(len(images), len(specs))
        if spec_count <= 0:
            return
        spec_start = max(0, (len(specs) - spec_count) // 2)
        visible_specs = specs[spec_start : spec_start + spec_count]
        visible_images = images[:spec_count]
        focus_index = max(range(len(visible_specs)), key=lambda idx: int(visible_specs[idx].get("elevation", 0)))
        ordering = list(range(spec_count)) if strict_poster_row else sorted(range(spec_count), key=lambda idx: int(visible_specs[idx].get("elevation", 0)))
        angle_tune = 0.0 if strict_poster_row else (max(0, min(poster_rotation, 100)) - 50) * angle_tune_scale
        for idx in ordering:
            spec = visible_specs[idx]
            size = spec["size"]
            base_angle = float(spec.get("rotation", 0.0))
            radius = int(spec.get("radius", 22))
            glow_alpha = int(spec.get("glow_alpha", 72 if idx != focus_index else 118))
            glow_color = tone["accent"] if strict_poster_row else (tone["glow"] if idx == focus_index else tone["accent"])
            card = self._create_showcase_poster_card(
                visible_images[idx],
                size=size,
                rotation=base_angle + angle_tune,
                radius=radius,
                glow_color=glow_color,
                glow_alpha=glow_alpha,
            )
            origin = spec["origin"]
            self._paste_with_reflection(
                canvas,
                card,
                origin,
                opacity=float(spec.get("reflection_opacity", reflection_opacity)),
                blur_radius=int(spec.get("reflection_blur", 11)),
                scale=float(spec.get("reflection_scale", reflection_scale)),
            )

    def _draw_showcase_title_block(
        self,
        *,
        draw: ImageDraw.ImageDraw,
        title_text: str,
        subtitle_text: str,
        title_font: ImageFont.ImageFont,
        subtitle_font: ImageFont.ImageFont,
        align: str,
        x_bounds: tuple[int, int],
        base_y: int,
        title_y_offset: int,
        tone: dict[str, tuple[int, int, int]],
        decor: dict[str, Any],
    ) -> None:
        left, right = x_bounds
        safe_align = _normalize_title_align(align, fallback="left")
        title_box = draw.textbbox((0, 0), title_text, font=title_font)
        title_width = title_box[2] - title_box[0]
        title_x = _aligned_x(left, right, title_width, safe_align)
        title_y = base_y + title_y_offset
        title_height = title_box[3] - title_box[1]
        self._draw_text_shadow(
            draw,
            (title_x, title_y),
            title_text,
            title_font,
            fill=(255, 255, 255, 255),
            shadow=(0, 0, 0, 168),
        )
        marker_style = str(decor.get("marker") or "").strip().lower()
        line_y = title_y + title_height + int(decor.get("line_gap", 18))
        line_height = int(decor.get("line_height", 5))
        if marker_style == "vertical":
            marker_gap = int(decor.get("marker_gap", 26))
            marker_width = int(decor.get("marker_width", max(4, line_height)))
            marker_x = title_x - marker_gap
            marker_top = title_y + max(2, title_height // 8)
            marker_bottom = title_y + title_height + int(decor.get("subtitle_gap", 18)) + max(18, title_height // 3)
            draw.rounded_rectangle(
                (marker_x, marker_top, marker_x + marker_width, marker_bottom),
                radius=max(2, marker_width // 2),
                fill=(tone["accent"][0], tone["accent"][1], tone["accent"][2], 220),
            )
            subtitle_y = title_y + title_height + int(decor.get("subtitle_gap", 18))
        else:
            line_length = int(decor.get("line_length", 72))
            line_radius = max(2, line_height // 2)
            line_x = _aligned_x(left, right, line_length, safe_align)
            draw.rounded_rectangle(
                (line_x, line_y, line_x + line_length, line_y + line_height),
                radius=line_radius,
                fill=(tone["accent"][0], tone["accent"][1], tone["accent"][2], 235),
            )
            subtitle_y = line_y + line_height + int(decor.get("subtitle_gap", 18))
        if subtitle_text:
            subtitle_box = draw.textbbox((0, 0), subtitle_text, font=subtitle_font)
            subtitle_width = subtitle_box[2] - subtitle_box[0]
            subtitle_x = _aligned_x(left, right, subtitle_width, safe_align)
            self._draw_text_shadow(
                draw,
                (subtitle_x, subtitle_y),
                subtitle_text,
                subtitle_font,
                fill=(tone["soft"][0], tone["soft"][1], tone["soft"][2], 238),
                shadow=(0, 0, 0, 144),
            )

    def _build_background(
        self,
        source_image: Image.Image,
        size: tuple[int, int],
        *,
        overlay_strength: int,
        tone: dict[str, tuple[int, int, int]],
        left_weight: float,
    ) -> Image.Image:
        width, height = size
        background = ImageOps_fit(source_image, size)
        background = background.filter(ImageFilter.GaussianBlur(radius=12))
        if overlay_strength <= 0:
            return background.convert("RGBA")
        overlay = Image.new("RGBA", (width, height), (14, 20, 28, 0))
        draw = ImageDraw.Draw(overlay)
        base_alpha = 38 + int(max(0, min(overlay_strength, 100)) * 1.2)
        left_alpha = min(220, base_alpha + 64)
        accent = tone["accent"]
        glow = tone["glow"]
        draw.rectangle((0, 0, width, height), fill=(8, 16, 26, base_alpha))
        draw.rectangle((0, 0, int(width * left_weight), height), fill=(6, 14, 22, left_alpha))
        draw.ellipse((-180, height - 280, 520, height + 220), fill=(glow[0], glow[1], glow[2], 44))
        draw.ellipse((width - 520, -240, width + 180, 340), fill=(accent[0], accent[1], accent[2], 42))
        overlay = overlay.filter(ImageFilter.GaussianBlur(radius=4))
        return Image.alpha_composite(background.convert("RGBA"), overlay)

    def _build_showcase_stage_background(
        self,
        *,
        size: tuple[int, int],
        tone: dict[str, tuple[int, int, int]],
        overlay_strength: int,
        title_area_ratio: float,
    ) -> Image.Image:
        width, height = size
        background = Image.new("RGBA", size, (0, 0, 0, 255))
        top_color = (6, 10, 18)
        mid_color = (8, 14, 24)
        floor_color = (16, 14, 18)
        draw = ImageDraw.Draw(background)
        split_y = int(height * 0.7)
        for y in range(height):
            if y <= split_y:
                ratio = y / max(1, split_y)
                color = tuple(int(top_color[idx] + (mid_color[idx] - top_color[idx]) * ratio) for idx in range(3))
            else:
                ratio = (y - split_y) / max(1, height - split_y)
                color = tuple(int(mid_color[idx] + (floor_color[idx] - mid_color[idx]) * ratio) for idx in range(3))
            draw.line((0, y, width, y), fill=color + (255,))

        if overlay_strength <= 0:
            return background

        overlay = Image.new("RGBA", size, (0, 0, 0, 0))
        overlay_draw = ImageDraw.Draw(overlay)
        left_width = int(width * title_area_ratio)
        darkness = min(220, 92 + int(max(0, min(overlay_strength, 100)) * 0.9))
        overlay_draw.rectangle((0, 0, left_width, height), fill=(4, 9, 16, darkness))
        overlay_draw.rectangle((left_width, 0, width, height), fill=(6, 10, 16, 42))
        overlay_draw.ellipse((width - 520, 42, width - 80, 500), fill=(tone["glow"][0], tone["glow"][1], tone["glow"][2], 54))
        overlay_draw.ellipse((width - 268, 110, width + 112, 720), fill=(tone["accent"][0], tone["accent"][1], tone["accent"][2], 32))
        overlay = overlay.filter(ImageFilter.GaussianBlur(radius=14))
        background = Image.alpha_composite(background, overlay)

        beams = Image.new("RGBA", size, (0, 0, 0, 0))
        beam_draw = ImageDraw.Draw(beams)
        beam_draw.polygon(
            [
                (width - 330, -20),
                (width - 270, -20),
                (width - 520, split_y + 90),
                (width - 410, split_y + 90),
            ],
            fill=(tone["glow"][0], tone["glow"][1], tone["glow"][2], 78),
        )
        beam_draw.polygon(
            [
                (width - 110, 10),
                (width - 72, 10),
                (width - 248, split_y + 80),
                (width - 178, split_y + 80),
            ],
            fill=(tone["accent"][0], tone["accent"][1], tone["accent"][2], 68),
        )
        beam_draw.ellipse((width - 338, -30, width - 236, 76), fill=(255, 255, 255, 95))
        beam_draw.ellipse((width - 124, -12, width - 54, 58), fill=(255, 226, 182, 72))
        beams = beams.filter(ImageFilter.GaussianBlur(radius=24))
        background = Image.alpha_composite(background, beams)

        floor = Image.new("RGBA", size, (0, 0, 0, 0))
        floor_draw = ImageDraw.Draw(floor)
        floor_draw.rectangle((0, split_y, width, height), fill=(255, 255, 255, 10))
        floor_draw.ellipse((width - 620, split_y - 18, width - 40, height + 120), fill=(tone["accent"][0], tone["accent"][1], tone["accent"][2], 34))
        floor_draw.ellipse((width - 520, split_y + 26, width - 100, height + 160), fill=(255, 196, 112, 28))
        floor = floor.filter(ImageFilter.GaussianBlur(radius=28))
        background = Image.alpha_composite(background, floor)
        return background

    def _create_poster_card(
        self,
        image: Image.Image,
        *,
        size: tuple[int, int],
        rotation: float,
        radius: int,
    ) -> Image.Image:
        poster = ImageOps_fit(image, size)
        poster_layer = Image.new("RGBA", size, (0, 0, 0, 0))
        poster_layer.paste(poster, (0, 0))
        mask = Image.new("L", size, 0)
        ImageDraw.Draw(mask).rounded_rectangle((0, 0, size[0], size[1]), radius=radius, fill=255)
        rounded = Image.new("RGBA", size, (0, 0, 0, 0))
        rounded.paste(poster_layer, (0, 0), mask)
        shadow_box = Image.new("RGBA", (size[0] + 32, size[1] + 32), (0, 0, 0, 0))
        shadow_draw = ImageDraw.Draw(shadow_box)
        shadow_draw.rounded_rectangle((14, 14, size[0] + 10, size[1] + 10), radius=radius + 4, fill=(0, 0, 0, 92))
        shadow_box = shadow_box.filter(ImageFilter.GaussianBlur(radius=12))
        card = Image.new("RGBA", shadow_box.size, (0, 0, 0, 0))
        card.alpha_composite(shadow_box, (0, 0))
        card.alpha_composite(rounded, (16, 16))
        if abs(rotation) > 0.1:
            return card.rotate(rotation, resample=Image.Resampling.BICUBIC, expand=True)
        return card

    def _create_showcase_poster_card(
        self,
        image: Image.Image,
        *,
        size: tuple[int, int],
        rotation: float,
        radius: int,
        glow_color: tuple[int, int, int],
        glow_alpha: int,
    ) -> Image.Image:
        base = self._create_poster_card(image, size=size, rotation=rotation, radius=radius)
        alpha = base.getchannel("A")
        glow = Image.new("RGBA", base.size, glow_color + (0,))
        glow.putalpha(alpha.point(lambda value: min(glow_alpha, int(value * 0.54))))
        glow = glow.filter(ImageFilter.GaussianBlur(radius=12))
        sheen = Image.new("RGBA", base.size, (0, 0, 0, 0))
        sheen_draw = ImageDraw.Draw(sheen)
        sheen_draw.rectangle((0, 0, base.width, max(1, int(base.height * 0.22))), fill=(255, 255, 255, 18))
        sheen = sheen.filter(ImageFilter.GaussianBlur(radius=10))
        card = Image.new("RGBA", base.size, (0, 0, 0, 0))
        card.alpha_composite(glow, (0, 0))
        card.alpha_composite(base, (0, 0))
        card.alpha_composite(sheen, (0, 0))
        return card

    def _paste_with_reflection(
        self,
        canvas: Image.Image,
        overlay: Image.Image,
        origin: tuple[int, int],
        *,
        opacity: float,
        blur_radius: int,
        scale: float,
    ) -> None:
        self._alpha_paste(canvas, overlay, origin)
        reflected = overlay.transpose(Image.Transpose.FLIP_TOP_BOTTOM)
        reflected = reflected.resize(
            (overlay.width, max(1, int(overlay.height * scale))),
            Image.Resampling.BICUBIC,
        )
        alpha = reflected.getchannel("A")
        merged_alpha = Image.new("L", reflected.size, 0)
        for y in range(reflected.height):
            ratio = 1.0 - (y / max(1, reflected.height - 1))
            value = int(255 * max(0.0, min(1.0, opacity * ratio)))
            row = alpha.crop((0, y, reflected.width, y + 1)).point(lambda pixel, limit=value: min(limit, pixel))
            merged_alpha.paste(row, (0, y))
        reflected.putalpha(merged_alpha)
        reflected = reflected.filter(ImageFilter.GaussianBlur(radius=blur_radius))
        reflection_origin = (origin[0], origin[1] + overlay.height - 22)
        self._alpha_paste(canvas, reflected, reflection_origin)

    def _draw_title_block(
        self,
        *,
        draw: ImageDraw.ImageDraw,
        title_text: str,
        subtitle_text: str,
        title_font: ImageFont.ImageFont,
        subtitle_font: ImageFont.ImageFont,
        align: str,
        x_bounds: tuple[int, int],
        base_y: int,
        title_y_offset: int,
        tone: dict[str, tuple[int, int, int]],
    ) -> None:
        left, right = x_bounds
        safe_align = _normalize_title_align(align, fallback="left")
        title_box = draw.textbbox((0, 0), title_text, font=title_font)
        title_width = title_box[2] - title_box[0]
        title_x = _aligned_x(left, right, title_width, safe_align)
        title_y = base_y + title_y_offset
        subtitle_y = title_y + (title_box[3] - title_box[1]) + 18
        self._draw_text_shadow(
            draw,
            (title_x, title_y),
            title_text,
            title_font,
            fill=(255, 255, 255, 255),
            shadow=(0, 0, 0, 156),
        )
        if subtitle_text:
            subtitle_box = draw.textbbox((0, 0), subtitle_text, font=subtitle_font)
            subtitle_width = subtitle_box[2] - subtitle_box[0]
            subtitle_x = _aligned_x(left, right, subtitle_width, safe_align)
            self._draw_text_shadow(
                draw,
                (subtitle_x, subtitle_y),
                subtitle_text,
                subtitle_font,
                fill=(tone["soft"][0], tone["soft"][1], tone["soft"][2], 235),
                shadow=(0, 0, 0, 138),
            )

    def _load_font(self, font_key: str, font_size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
        font_map = {row["key"]: row["path"] for row in available_cover_fonts()}
        font_path = font_map.get(str(font_key or "").strip()) or ""
        try:
            if font_path:
                return ImageFont.truetype(font_path, font_size)
        except Exception:
            pass
        return ImageFont.load_default()

    @staticmethod
    def _draw_text_shadow(
        draw: ImageDraw.ImageDraw,
        pos: tuple[int, int],
        text: str,
        font: ImageFont.ImageFont,
        *,
        fill: tuple[int, int, int, int],
        shadow: tuple[int, int, int, int],
    ) -> None:
        x, y = pos
        for dx, dy in ((0, 3), (2, 2), (-2, 2)):
            draw.text((x + dx, y + dy), text, font=font, fill=shadow)
        draw.text(pos, text, font=font, fill=fill)

    @staticmethod
    def _alpha_paste(canvas: Image.Image, overlay: Image.Image, origin: tuple[int, int]) -> None:
        canvas.alpha_composite(overlay, origin)


def _normalize_backup_meta(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    return {
        "path": str(source.get("path") or "").strip(),
        "contentType": str(source.get("contentType") or "image/png").strip() or "image/png",
    }


def _normalize_view_ids(value: Any, *, fallback: Any = "") -> list[str]:
    source = value if isinstance(value, list) else [fallback]
    rows: list[str] = []
    seen: set[str] = set()
    for item in source:
        view_id = str(item or "").strip()
        if not view_id or view_id in seen:
            continue
        seen.add(view_id)
        rows.append(view_id)
    return rows[:30]


def _cover_font_dir() -> pathlib.Path:
    raw = str(os.environ.get("VISTAMIRROR_COVER_FONT_DIR") or "").strip()
    return pathlib.Path(raw).expanduser() if raw else DEFAULT_CUSTOM_COVER_FONT_DIR


def _discover_custom_cover_fonts() -> list[dict[str, str]]:
    root = _cover_font_dir()
    if not root.exists():
        return []
    manifest_path = root / "fonts.json"
    if manifest_path.exists():
        return _load_custom_cover_fonts_from_manifest(root, manifest_path)
    return _scan_custom_cover_fonts(root)


def _load_custom_cover_fonts_from_manifest(root: pathlib.Path, manifest_path: pathlib.Path) -> list[dict[str, str]]:
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return _scan_custom_cover_fonts(root)
    if not isinstance(payload, list):
        return _scan_custom_cover_fonts(root)
    rows: list[dict[str, str]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        relative_path = str(item.get("path") or "").strip()
        if not relative_path:
            continue
        font_path = (root / relative_path).resolve()
        try:
            font_path.relative_to(root.resolve())
        except Exception:
            continue
        if font_path.suffix.lower() not in CUSTOM_COVER_FONT_EXTENSIONS:
            continue
        label = str(item.get("label") or "").strip() or _humanize_font_name(font_path.stem)
        key = _normalize_font_key(item.get("key") or font_path.stem)
        rows.append({"key": key, "label": label, "path": str(font_path)})
    return rows


def _scan_custom_cover_fonts(root: pathlib.Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in CUSTOM_COVER_FONT_EXTENSIONS:
            continue
        rows.append(
            {
                "key": _normalize_font_key(path.stem),
                "label": _humanize_font_name(path.stem),
                "path": str(path),
            }
        )
    return rows


def _normalize_font_key(value: Any) -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9]+", "_", raw)
    raw = re.sub(r"_+", "_", raw).strip("_")
    return raw or "custom_font"


def _humanize_font_name(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "自定义字体"
    normalized = raw.replace("_", " ").replace("-", " ")
    normalized = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or raw


def _clamp_int(value: Any, *, fallback: int, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except Exception:
        number = int(fallback)
    if number < minimum:
        number = minimum
    if number > maximum:
        number = maximum
    return number


def _mode_meta(template_key: str) -> dict[str, Any]:
    safe_key = _normalize_template_key(template_key)
    for mode in cover_studio_modes():
        if mode["key"] == safe_key:
            return mode
    return cover_studio_modes()[0]


def _mode_defaults(template_key: str) -> dict[str, Any]:
    return dict(_mode_meta(template_key).get("defaults") or {})


def _normalize_template_key(value: Any) -> str:
    raw = str(value or "").strip()
    valid = {mode["key"] for mode in cover_studio_modes()}
    return raw if raw in valid else DEFAULT_TEMPLATE_KEY


def _normalize_title_align(value: Any, *, fallback: str) -> str:
    raw = str(value or "").strip().lower()
    return raw if raw in {"left", "center", "right"} else fallback


def _normalize_accent_tone(value: Any, *, fallback: str) -> str:
    raw = str(value or "").strip().lower()
    valid = {tone["key"] for tone in cover_studio_accent_tones()}
    return raw if raw in valid else fallback


def _tone_palette(accent_tone: str) -> dict[str, tuple[int, int, int]]:
    tones = {
        "blue": {"accent": (108, 190, 255), "glow": (84, 220, 232), "soft": (232, 243, 255)},
        "gold": {"accent": (255, 194, 114), "glow": (255, 214, 144), "soft": (248, 240, 224)},
        "emerald": {"accent": (88, 220, 176), "glow": (108, 230, 196), "soft": (226, 245, 238)},
        "rose": {"accent": (255, 154, 176), "glow": (255, 182, 196), "soft": (250, 230, 236)},
        "neutral": {"accent": (194, 206, 218), "glow": (176, 188, 204), "soft": (236, 240, 244)},
    }
    return tones.get(_normalize_accent_tone(accent_tone, fallback="blue"), tones["blue"])


def _cinematic_showcase_variant(variant: str, *, thumb: bool) -> dict[str, Any]:
    return get_cinematic_showcase_variant(variant, thumb=thumb)


def _rotation_from_strength(strength: int, *, base: float) -> float:
    return base + (max(0, min(strength, 100)) - 50) * 0.12


def _aligned_x(left: int, right: int, content_width: int, align: str) -> int:
    if align == "center":
        return left + max(0, (right - left - content_width) // 2)
    if align == "right":
        return max(left, right - content_width)
    return left


def ImageOps_fit(image: Image.Image, size: tuple[int, int]) -> Image.Image:
    target_w, target_h = size
    source = image.convert("RGB")
    scale = max(target_w / max(1, source.width), target_h / max(1, source.height))
    resized = source.resize((max(1, int(source.width * scale)), max(1, int(source.height * scale))), Image.Resampling.LANCZOS)
    left = max(0, (resized.width - target_w) // 2)
    top = max(0, (resized.height - target_h) // 2)
    return resized.crop((left, top, left + target_w, top + target_h))
