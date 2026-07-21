from __future__ import annotations

import json
import os
import pathlib
from typing import Any, Callable


def _normalize_optional_mapping(raw: Any) -> dict[str, Any]:
    """Keep older store callers compatible while optional integrations are absent."""
    return dict(raw) if isinstance(raw, dict) else {}


def _default_optional_mapping() -> dict[str, Any]:
    return {}


def default_store_payload(
    *,
    default_notification_config: Callable[[], dict[str, Any]],
    default_bot_config: Callable[[], dict[str, Any]],
    default_ai_config: Callable[[], dict[str, Any]],
    default_moviepilot_config: Callable[[], dict[str, Any]] = _default_optional_mapping,
    default_cover_studio_config: Callable[[], dict[str, Any]],
    default_drive115_config: Callable[[], dict[str, Any]],
    default_hdhive_config: Callable[[], dict[str, Any]],
    default_library_directory_config: Callable[[], dict[str, Any]],
    sync_notification_config_to_bot_config: Callable[[Any, Any], dict[str, Any]],
) -> dict[str, Any]:
    notification_config = default_notification_config()
    return {
        "embyConfig": {},
        "invites": [],
        "botConfig": sync_notification_config_to_bot_config(notification_config, default_bot_config()),
        "notificationConfig": notification_config,
        "aiConfig": default_ai_config(),
        "moviePilotConfig": default_moviepilot_config(),
        "coverStudioConfig": default_cover_studio_config(),
        "drive115Config": default_drive115_config(),
        "hdhiveConfig": default_hdhive_config(),
        "libraryDirectoryConfig": default_library_directory_config(),
    }


def store_path(
    *,
    base_dir: pathlib.Path,
    data_dir: pathlib.Path,
    store_file_name: str,
    legacy_store_file_name: str,
) -> pathlib.Path:
    current = data_dir / store_file_name
    legacy_current = base_dir / store_file_name
    legacy = base_dir / legacy_store_file_name
    if current.exists():
        return current
    current.parent.mkdir(parents=True, exist_ok=True)
    if legacy_current.exists():
        try:
            os.replace(legacy_current, current)
            return current
        except OSError:
            return legacy_current
    if legacy.exists():
        try:
            os.replace(legacy, current)
            return current
        except OSError:
            return legacy
    return current


def read_store_unlocked(
    *,
    path: pathlib.Path,
    default_store_factory: Callable[[], dict[str, Any]],
    normalize_bot_config: Callable[[Any], dict[str, Any]],
    normalize_notification_config: Callable[..., dict[str, Any]],
    sync_notification_config_to_bot_config: Callable[[Any, Any], dict[str, Any]],
    normalize_ai_config: Callable[[Any], dict[str, Any]],
    normalize_moviepilot_config: Callable[[Any], dict[str, Any]] = _normalize_optional_mapping,
    normalize_cover_studio_config: Callable[[Any], dict[str, Any]],
    normalize_drive115_config: Callable[[Any], dict[str, Any]],
    normalize_hdhive_config: Callable[[Any], dict[str, Any]],
    normalize_library_directory_config: Callable[[Any], dict[str, Any]],
) -> dict[str, Any]:
    if not path.exists():
        return default_store_factory()

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default_store_factory()

    emby_config = data.get("embyConfig") if isinstance(data, dict) else {}
    invites = data.get("invites") if isinstance(data, dict) else []
    raw_bot_config = data.get("botConfig") if isinstance(data, dict) else {}
    bot_config = normalize_bot_config(raw_bot_config)
    notification_config = (
        normalize_notification_config(data.get("notificationConfig"), legacy_bot_config=bot_config)
        if isinstance(data, dict)
        else default_store_factory()["notificationConfig"]
    )
    bot_config = sync_notification_config_to_bot_config(notification_config, bot_config)
    ai_config = data.get("aiConfig") if isinstance(data, dict) else {}
    moviepilot_config = data.get("moviePilotConfig") if isinstance(data, dict) else {}
    cover_studio_config = data.get("coverStudioConfig") if isinstance(data, dict) else {}
    drive115_config = data.get("drive115Config") if isinstance(data, dict) else {}
    hdhive_config = data.get("hdhiveConfig") if isinstance(data, dict) else {}
    library_directory_config = data.get("libraryDirectoryConfig") if isinstance(data, dict) else {}
    if not isinstance(emby_config, dict):
        emby_config = {}
    if not isinstance(invites, list):
        invites = []
    return {
        "embyConfig": emby_config,
        "invites": invites,
        "botConfig": normalize_bot_config(bot_config),
        "notificationConfig": notification_config,
        "aiConfig": normalize_ai_config(ai_config),
        "moviePilotConfig": normalize_moviepilot_config(moviepilot_config),
        "coverStudioConfig": normalize_cover_studio_config(cover_studio_config),
        "drive115Config": normalize_drive115_config(drive115_config),
        "hdhiveConfig": normalize_hdhive_config(hdhive_config),
        "libraryDirectoryConfig": normalize_library_directory_config(library_directory_config),
    }


def write_store_unlocked(*, path: pathlib.Path, store: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    payload = json.dumps(store, ensure_ascii=False, indent=2)
    tmp.write_text(payload, encoding="utf-8")
    os.replace(tmp, path)
