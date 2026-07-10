from __future__ import annotations

import json
from typing import Any, Callable


def handle_bot_config_get(
    handler: Any,
    *,
    store_lock: Any,
    read_store: Callable[[], dict[str, Any]],
    write_store: Callable[[dict[str, Any]], None],
    store_path: Callable[[], Any],
    apply_bot_env_overrides: Callable[[Any], dict[str, Any]],
    apply_notification_env_overrides: Callable[..., dict[str, Any]],
    env_controlled_fields_payload: Callable[[], dict[str, list[str]]],
) -> None:
    with store_lock:
        store = read_store()
        path = store_path()
        needs_persist = True
        if path.exists():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                needs_persist = (
                    not isinstance(raw, dict)
                    or not isinstance(raw.get("botConfig"), dict)
                    or not isinstance(raw.get("notificationConfig"), dict)
                )
            except Exception:
                needs_persist = True
        if needs_persist:
            write_store(store)

        bot_config = apply_bot_env_overrides(store.get("botConfig"))
        notification_config = apply_notification_env_overrides(
            store.get("notificationConfig"),
            legacy_bot_config=store.get("botConfig"),
        )

    managed = env_controlled_fields_payload()
    handler._send_json(
        200,
        {
            "ok": True,
            "botConfig": bot_config,
            "notificationConfig": notification_config,
            "envControlledFields": managed,
            "managedByEnv": managed,
        },
    )

def handle_bot_config_save(
    handler: Any,
    *,
    store_lock: Any,
    read_store: Callable[[], dict[str, Any]],
    write_store: Callable[[dict[str, Any]], None],
    validate_bot_config: Callable[[Any], tuple[dict[str, Any] | None, str | None]],
    normalize_bot_config: Callable[[Any], dict[str, Any]],
    env_managed_bot_fields: Callable[[], list[str]],
    sync_bot_config_into_notification: Callable[[Any, Any], dict[str, Any]],
    apply_bot_env_overrides: Callable[[Any], dict[str, Any]],
    apply_notification_env_overrides: Callable[..., dict[str, Any]],
    env_controlled_fields_payload: Callable[[], dict[str, list[str]]],
    redact_sensitive: Callable[[Any], Any],
    telegram_wakeup: Callable[[], None] | None,
) -> None:
    payload = handler._read_json_body()
    if payload is None:
        return

    raw_bot_config = payload.get("botConfig")
    if raw_bot_config is None:
        raw_bot_config = payload

    bot_config, error = validate_bot_config(raw_bot_config)
    if error:
        handler._send_json(400, {"error": error})
        return
    if bot_config is None:
        handler._send_json(400, {"error": "机器人配置无效"})
        return

    with store_lock:
        store = read_store()
        current = normalize_bot_config(store.get("botConfig"))
        locked = env_managed_bot_fields()
        if locked:
            for field in locked:
                bot_config[field] = current.get(field)
        store["botConfig"] = normalize_bot_config(bot_config)
        store["notificationConfig"] = sync_bot_config_into_notification(
            store.get("notificationConfig"),
            store["botConfig"],
        )
        write_store(store)
        saved_config = apply_bot_env_overrides(store.get("botConfig"))
        saved_notification_config = apply_notification_env_overrides(
            store.get("notificationConfig"),
            legacy_bot_config=store.get("botConfig"),
        )

    if telegram_wakeup is not None:
        telegram_wakeup()

    handler._log_event(
        level="info",
        module="system",
        action="bot_config_saved",
        message="机器人配置已保存。",
        status=200,
        detail={"changedFields": sorted(redact_sensitive(raw_bot_config).keys()) if isinstance(raw_bot_config, dict) else []},
    )

    managed = env_controlled_fields_payload()
    handler._send_json(
        200,
        {
            "ok": True,
            "botConfig": saved_config,
            "notificationConfig": saved_notification_config,
            "envControlledFields": managed,
            "managedByEnv": managed,
        },
    )


def handle_ai_config_get(
    handler: Any,
    *,
    store_lock: Any,
    read_store: Callable[[], dict[str, Any]],
    write_store: Callable[[dict[str, Any]], None],
    store_path: Callable[[], Any],
    apply_ai_env_overrides: Callable[[Any], dict[str, Any]],
    env_controlled_fields_payload: Callable[[], dict[str, list[str]]],
) -> None:
    with store_lock:
        store = read_store()
        path = store_path()
        needs_persist = True
        if path.exists():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                needs_persist = not isinstance(raw, dict) or not isinstance(raw.get("aiConfig"), dict)
            except Exception:
                needs_persist = True
        if needs_persist:
            write_store(store)
        ai_config = apply_ai_env_overrides(store.get("aiConfig"))

    managed = env_controlled_fields_payload()
    handler._send_json(
        200,
        {
            "ok": True,
            "aiConfig": ai_config,
            "envControlledFields": managed,
            "managedByEnv": managed,
        },
    )


def handle_ai_config_save(
    handler: Any,
    *,
    store_lock: Any,
    read_store: Callable[[], dict[str, Any]],
    write_store: Callable[[dict[str, Any]], None],
    apply_ai_env_overrides: Callable[[Any], dict[str, Any]],
    env_managed_ai_fields: Callable[[], list[str]],
    validate_ai_config: Callable[[Any], tuple[dict[str, Any] | None, str | None]],
    normalize_ai_config: Callable[[Any], dict[str, Any]],
    env_controlled_fields_payload: Callable[[], dict[str, list[str]]],
    redact_sensitive: Callable[[Any], Any],
    telegram_wakeup: Callable[[], None] | None,
) -> None:
    payload = handler._read_json_body()
    if payload is None:
        return

    raw_ai_config = payload.get("aiConfig")
    if raw_ai_config is None:
        raw_ai_config = payload

    if not isinstance(raw_ai_config, dict):
        handler._send_json(400, {"error": "AI 配置必须是对象"})
        return

    with store_lock:
        current_store = read_store()
        current_config = apply_ai_env_overrides(current_store.get("aiConfig"))
        locked = env_managed_ai_fields()
    if locked:
        raw_ai_config = dict(raw_ai_config)
        for field in locked:
            raw_ai_config[field] = current_config.get(field)

    ai_config, error = validate_ai_config(raw_ai_config)
    if error:
        handler._send_json(400, {"error": error})
        return
    if ai_config is None:
        handler._send_json(400, {"error": "AI 配置无效"})
        return

    with store_lock:
        store = read_store()
        current = normalize_ai_config(store.get("aiConfig"))
        locked = env_managed_ai_fields()
        if locked:
            for field in locked:
                ai_config[field] = current.get(field)
        store["aiConfig"] = normalize_ai_config(ai_config)
        write_store(store)
        saved_config = apply_ai_env_overrides(store.get("aiConfig"))

    if telegram_wakeup is not None:
        telegram_wakeup()

    handler._log_event(
        level="info",
        module="system",
        action="ai_config_saved",
        message="AI 助手配置已保存。",
        status=200,
        detail={"changedFields": sorted(redact_sensitive(raw_ai_config).keys()) if isinstance(raw_ai_config, dict) else []},
    )

    managed = env_controlled_fields_payload()
    handler._send_json(
        200,
        {
            "ok": True,
            "aiConfig": saved_config,
            "envControlledFields": managed,
            "managedByEnv": managed,
        },
    )
