from __future__ import annotations

from typing import Any, Callable


def handle_invite_sync_status(
    handler: Any,
    *,
    store_lock: Any,
    read_store: Callable[[], dict[str, Any]],
    apply_emby_env_overrides: Callable[[Any], dict[str, Any]],
    normalize_library_directory_config: Callable[[Any], dict[str, Any]],
    env_controlled_fields_payload: Callable[[], dict[str, list[str]]],
    invite_to_public: Callable[[dict[str, Any]], dict[str, Any]],
    now_iso: Callable[[], str],
) -> None:
    with store_lock:
        store = read_store()
        invites = store.get("invites", [])
        if not isinstance(invites, list):
            invites = []
        emby_config = apply_emby_env_overrides(store.get("embyConfig"))
        library_directory_config = normalize_library_directory_config(store.get("libraryDirectoryConfig"))
        rows = [invite_to_public(item) for item in invites if isinstance(item, dict)]

    rows.sort(key=lambda item: str(item.get("createdAt") or ""), reverse=True)
    active_count = sum(1 for item in rows if item.get("statusCode") == "active")
    used_count = sum(1 for item in rows if item.get("statusCode") == "used")
    managed = env_controlled_fields_payload()
    handler._send_json(
        200,
        {
            "ok": True,
            "synced": True,
            "inviteCount": len(rows),
            "activeCount": active_count,
            "usedCount": used_count,
            "invites": rows,
            "embyConfig": emby_config,
            "libraryDirectoryConfig": library_directory_config,
            "envControlledFields": managed,
            "managedByEnv": managed,
            "updatedAt": now_iso(),
        },
    )


def handle_invite_sync(
    handler: Any,
    *,
    store_lock: Any,
    read_store: Callable[[], dict[str, Any]],
    write_store: Callable[[dict[str, Any]], None],
    sanitize_invite_record: Callable[[dict[str, Any]], dict[str, Any] | None],
    normalize_library_directory_config: Callable[[Any], dict[str, Any]],
    merge_emby_config_for_save: Callable[[Any, Any], dict[str, Any]],
    env_managed_emby_fields: Callable[[], list[str]],
    merge_invites: Callable[[list[dict[str, Any]], list[dict[str, Any]]], list[dict[str, Any]]],
    apply_emby_env_overrides: Callable[[Any], dict[str, Any]],
    env_controlled_fields_payload: Callable[[], dict[str, list[str]]],
) -> None:
    payload = handler._read_json_body()
    if payload is None:
        return

    emby_config_raw = payload.get("embyConfig")
    invites_raw = payload.get("invites")
    library_directory_config_raw = payload.get("libraryDirectoryConfig")

    if not isinstance(emby_config_raw, dict):
        handler._send_json(400, {"error": "embyConfig must be an object"})
        return
    if not isinstance(invites_raw, list):
        handler._send_json(400, {"error": "invites must be an array"})
        return
    if library_directory_config_raw is not None and not isinstance(library_directory_config_raw, dict):
        handler._send_json(400, {"error": "libraryDirectoryConfig must be an object"})
        return

    sanitized_invites: list[dict[str, Any]] = []
    for invite in invites_raw:
        sanitized = sanitize_invite_record(invite)
        if sanitized:
            sanitized_invites.append(sanitized)

    with store_lock:
        stored = read_store()
        current_emby_config = stored.get("embyConfig") if isinstance(stored.get("embyConfig"), dict) else {}
        current_library_directory_config = normalize_library_directory_config(stored.get("libraryDirectoryConfig"))

    emby_config = merge_emby_config_for_save(current_emby_config, emby_config_raw)
    library_directory_config = (
        normalize_library_directory_config(library_directory_config_raw)
        if library_directory_config_raw is not None
        else current_library_directory_config
    )

    with store_lock:
        store = read_store()
        current_emby_config = store.get("embyConfig") if isinstance(store.get("embyConfig"), dict) else {}
        locked = env_managed_emby_fields()
        if locked:
            for field in locked:
                emby_config[field] = str(current_emby_config.get(field) or "").strip()
        merged_invites = merge_invites(store.get("invites", []), sanitized_invites)
        store["embyConfig"] = emby_config
        store["invites"] = merged_invites
        store["libraryDirectoryConfig"] = library_directory_config
        write_store(store)
        effective_emby_config = apply_emby_env_overrides(store.get("embyConfig"))

    handler._log_event(
        level="info",
        module="system",
        action="server_config_synced",
        message="媒体服务器配置与邀请码已同步到后端。",
        status=200,
        detail={
            "inviteCount": len(sanitized_invites),
            "storedInviteCount": len(merged_invites),
            "changedFields": ["serverUrl", "apiKey", "clientName", "tmdbEnabled", "tmdbToken", "tmdbLanguage", "tmdbRegion", "libraryDirectoryConfig"],
            "envLockedFields": sorted(env_managed_emby_fields()),
            "tmdbConfigured": bool(effective_emby_config.get("tmdbToken")),
        },
    )
    managed = env_controlled_fields_payload()
    handler._send_json(
        200,
        {
            "ok": True,
            "message": "Invite store synced",
            "inviteCount": len(sanitized_invites),
            "storedInviteCount": len(merged_invites),
            "embyConfig": effective_emby_config,
            "libraryDirectoryConfig": library_directory_config,
            "envControlledFields": managed,
            "managedByEnv": managed,
        },
    )
