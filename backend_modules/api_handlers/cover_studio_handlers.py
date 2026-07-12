from __future__ import annotations

from typing import Any, Callable

from backend_modules.cover_studio_service import is_valid_cover_studio_cron, normalize_cover_studio_font_key


def handle_cover_studio_config_get(
    handler: Any,
    *,
    store_lock: Any,
    read_store: Callable[[], dict[str, Any]],
    write_store: Callable[[dict[str, Any]], None],
    normalize_cover_studio_config: Callable[[Any], dict[str, Any]],
    cover_studio_service: Any,
) -> None:
    with store_lock:
        store = read_store()
        config = normalize_cover_studio_config(store.get("coverStudioConfig"))
        if not isinstance(store.get("coverStudioConfig"), dict):
            store["coverStudioConfig"] = config
            write_store(store)
    handler._send_json(
        200,
        {
            "ok": True,
            "config": config,
            "fonts": cover_studio_service.list_fonts(),
            "modes": cover_studio_service.list_modes(),
            "accentTones": cover_studio_service.list_accent_tones(),
            "titleAlignOptions": cover_studio_service.list_title_align_options(),
            "pickModes": [
                {"key": "random", "label": "随机模式"},
                {"key": "recent", "label": "最近入库"},
            ],
        },
    )


def handle_cover_studio_config_save(
    handler: Any,
    *,
    store_lock: Any,
    read_store: Callable[[], dict[str, Any]],
    write_store: Callable[[dict[str, Any]], None],
    normalize_cover_studio_config: Callable[[Any], dict[str, Any]],
) -> None:
    payload = handler._read_json_body()
    if payload is None:
        return
    raw_config = payload.get("config") if isinstance(payload.get("config"), dict) else {}
    raw_schedule = raw_config.get("schedule") if isinstance(raw_config.get("schedule"), dict) else {}
    if raw_schedule.get("enabled") and not is_valid_cover_studio_cron(str(raw_schedule.get("cron") or "")):
        handler._send_json(400, {"error": "Cron 表达式无效，请使用 5 个字段，例如 */5 * * * *。"})
        return
    raw_schedules = raw_config.get("schedules") if isinstance(raw_config.get("schedules"), list) else []
    for item in raw_schedules:
        row = item if isinstance(item, dict) else {}
        if row.get("enabled") and not is_valid_cover_studio_cron(str(row.get("cron") or "")):
            handler._send_json(400, {"error": "封面计划的 Cron 表达式无效，请使用 5 个字段，例如 */5 * * * *。"})
            return
    config = normalize_cover_studio_config(raw_config)
    with store_lock:
        store = read_store()
        store["coverStudioConfig"] = config
        write_store(store)
    handler._send_json(200, {"ok": True, "config": config})


def handle_cover_studio_views_get(
    handler: Any,
    *,
    store_lock: Any,
    read_store: Callable[[], dict[str, Any]],
    apply_emby_env_overrides: Callable[[Any], dict[str, Any]],
    normalize_cover_studio_config: Callable[[Any], dict[str, Any]],
    build_emby_service: Callable[[dict[str, Any]], Any],
    cover_studio_service: Any,
) -> None:
    with store_lock:
        store = read_store()
        emby_config = apply_emby_env_overrides(store.get("embyConfig"))
        config = normalize_cover_studio_config(store.get("coverStudioConfig"))
    if not _require_emby_ready(handler, emby_config):
        return
    service = build_emby_service(emby_config)
    views = service.fetch_user_views()
    rows = []
    for view in views:
        row = dict(view)
        row["coverStudioStatus"] = cover_studio_service.build_view_status(view_id=str(view.get("id") or ""), config=config)
        rows.append(row)
    handler._send_json(200, {"ok": True, "views": rows})


def handle_cover_studio_status_get(
    handler: Any,
    *,
    query_params: dict[str, list[str]],
    store_lock: Any,
    read_store: Callable[[], dict[str, Any]],
    normalize_cover_studio_config: Callable[[Any], dict[str, Any]],
    cover_studio_service: Any,
) -> None:
    view_id = str((query_params.get("viewId") or [""])[0]).strip()
    if not view_id:
        handler._send_json(400, {"error": "缺少 viewId"})
        return
    with store_lock:
        store = read_store()
        config = normalize_cover_studio_config(store.get("coverStudioConfig"))
    handler._send_json(200, {"ok": True, "status": cover_studio_service.build_view_status(view_id=view_id, config=config)})


def handle_cover_studio_preview(
    handler: Any,
    *,
    store_lock: Any,
    read_store: Callable[[], dict[str, Any]],
    write_store: Callable[[dict[str, Any]], None],
    apply_emby_env_overrides: Callable[[Any], dict[str, Any]],
    normalize_cover_studio_config: Callable[[Any], dict[str, Any]],
    build_emby_service: Callable[[dict[str, Any]], Any],
    cover_studio_service: Any,
) -> None:
    payload = handler._read_json_body()
    if payload is None:
        return
    preview_only = bool(payload.get("previewOnly"))
    requested_view_ids = _request_view_ids(payload)
    template_key = str(payload.get("templateKey") or "stack_classic").strip() or "stack_classic"
    pick_mode = "recent" if str(payload.get("pickMode") or "").strip().lower() == "recent" else "random"
    font_key = normalize_cover_studio_font_key(payload.get("fontKey"))
    title_text = str(payload.get("titleText") or "").strip()
    subtitle_text = str(payload.get("subtitleText") or "").strip()
    title_font_size = payload.get("titleFontSize")
    subtitle_font_size = payload.get("subtitleFontSize")
    title_align = str(payload.get("titleAlign") or "left").strip() or "left"
    overlay_strength = payload.get("overlayStrength")
    poster_count = payload.get("posterCount")
    accent_tone = str(payload.get("accentTone") or "blue").strip() or "blue"
    poster_rotation = payload.get("posterRotation")
    title_y_offset = payload.get("titleYOffset")
    if not requested_view_ids:
        handler._send_json(400, {"error": "请至少选择一个目标媒体库视图"})
        return

    with store_lock:
        store = read_store()
        emby_config = apply_emby_env_overrides(store.get("embyConfig"))
        config = normalize_cover_studio_config(store.get("coverStudioConfig"))
    if not _require_emby_ready(handler, emby_config):
        return
    emby_service = build_emby_service(emby_config)
    views = emby_service.fetch_user_views()
    previews: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    resolved_view_ids: list[str] = []
    # A batch applies one cover per media library. Reusing the first library's
    # title would label every generated cover as the same library.
    use_per_view_titles = len(requested_view_ids) > 1
    for requested_view_id in requested_view_ids:
        target = _find_cover_view(views, requested_view_id)
        if not target:
            failures.append({"viewId": requested_view_id, "error": "未找到对应的 Emby 视图"})
            continue
        resolved_view_id = str(target.get("id") or requested_view_id).strip()
        browse_view_id = str(target.get("browseId") or resolved_view_id).strip() or resolved_view_id
        resolved_title = str(target.get("name") or "").strip()
        try:
            items = emby_service.fetch_view_items(view_id=browse_view_id, pick_mode=pick_mode)
            preview = cover_studio_service.generate_preview(
                view=target,
                items=items,
                template_key=template_key,
                font_key=font_key,
                title_text=resolved_title if use_per_view_titles else (title_text or resolved_title),
                subtitle_text=subtitle_text,
                title_font_size=title_font_size,
                subtitle_font_size=subtitle_font_size,
                title_align=title_align,
                overlay_strength=overlay_strength,
                poster_count=poster_count,
                accent_tone=accent_tone,
                poster_rotation=poster_rotation,
                title_y_offset=title_y_offset,
                emby_service=emby_service,
            )
        except Exception as err:
            failures.append({"viewId": resolved_view_id, "viewName": str(target.get("name") or "").strip(), "error": str(err)})
            continue
        resolved_view_ids.append(resolved_view_id)
        previews.append(
            {
                "viewId": resolved_view_id,
                "viewName": str(target.get("name") or "").strip(),
                "previewToken": preview.token,
                "previewDataUrl": preview.primary_image_data_url,
                "width": preview.primary_width,
                "height": preview.primary_height,
                "selectedItems": preview.selected_items,
            }
        )
    if not previews:
        message = failures[0]["error"] if failures else "未能生成媒体库封面预览"
        handler._send_json(422, {"error": message, "failures": failures})
        return
    primary_preview = previews[0]
    if not preview_only:
        config["lastViewId"] = resolved_view_ids[0]
        config["draft"].update(
            {
                "viewId": resolved_view_ids[0],
                "viewIds": resolved_view_ids,
                "templateKey": template_key,
                "pickMode": pick_mode,
                "titleText": title_text,
                "subtitleText": subtitle_text,
                "fontKey": font_key,
                "titleFontSize": _clamp_int(title_font_size, fallback=int(config["draft"].get("titleFontSize") or 108), minimum=56, maximum=180),
                "subtitleFontSize": _clamp_int(subtitle_font_size, fallback=int(config["draft"].get("subtitleFontSize") or 44), minimum=22, maximum=72),
                "titleAlign": title_align,
                "overlayStrength": _clamp_int(overlay_strength, fallback=int(config["draft"].get("overlayStrength") or 62), minimum=0, maximum=100),
                "posterCount": _clamp_int(poster_count, fallback=int(config["draft"].get("posterCount") or 5), minimum=2, maximum=8),
                "accentTone": accent_tone,
                "posterRotation": _clamp_int(poster_rotation, fallback=int(config["draft"].get("posterRotation") or 42), minimum=0, maximum=100),
                "titleYOffset": _clamp_int(title_y_offset, fallback=int(config["draft"].get("titleYOffset") or 0), minimum=-160, maximum=160),
            }
        )
        config = normalize_cover_studio_config(config)
        with store_lock:
            store = read_store()
            store["coverStudioConfig"] = config
            write_store(store)
    handler._send_json(
        200,
        {
            "ok": True,
            "view": _find_cover_view(views, resolved_view_ids[0]),
            "previewToken": primary_preview["previewToken"],
            "templateKey": template_key,
            "previewDataUrl": primary_preview["previewDataUrl"],
            "width": primary_preview["width"],
            "height": primary_preview["height"],
            "selectedItems": primary_preview["selectedItems"],
            "previews": previews,
            "failures": failures,
        },
    )


def handle_cover_studio_apply(
    handler: Any,
    *,
    store_lock: Any,
    read_store: Callable[[], dict[str, Any]],
    write_store: Callable[[dict[str, Any]], None],
    apply_emby_env_overrides: Callable[[Any], dict[str, Any]],
    normalize_cover_studio_config: Callable[[Any], dict[str, Any]],
    build_emby_service: Callable[[dict[str, Any]], Any],
    cover_studio_service: Any,
) -> None:
    payload = handler._read_json_body()
    if payload is None:
        return
    requested_items = _request_apply_items(payload)
    if not requested_items:
        handler._send_json(400, {"error": "缺少媒体库或封面预览"})
        return
    with store_lock:
        store = read_store()
        emby_config = apply_emby_env_overrides(store.get("embyConfig"))
        config = normalize_cover_studio_config(store.get("coverStudioConfig"))
    if not _require_emby_ready(handler, emby_config):
        return
    emby_service = build_emby_service(emby_config)
    views = emby_service.fetch_user_views()
    results: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for item in requested_items:
        view_id = item["viewId"]
        preview_token = item["previewToken"]
        target = _find_cover_view(views, view_id)
        resolved_view_id = str((target or {}).get("id") or view_id).strip()
        upload_target_id = str((target or {}).get("uploadTargetId") or resolved_view_id).strip() or resolved_view_id
        try:
            result = cover_studio_service.backup_and_apply(
                config=config,
                view_id=resolved_view_id,
                upload_view_id=upload_target_id,
                preview_token=preview_token,
                emby_service=emby_service,
            )
        except Exception as err:
            failures.append({"viewId": resolved_view_id, "error": str(err)})
            continue
        results.append({"viewId": resolved_view_id, "result": result})
    if not results:
        handler._send_json(422, {"error": failures[0]["error"] if failures else "未能应用封面", "failures": failures})
        return
    with store_lock:
        store = read_store()
        store["coverStudioConfig"] = config
        write_store(store)
    handler._send_json(
        200,
        {
            "ok": True,
            "result": results[0]["result"],
            "results": results,
            "failures": failures,
            "status": cover_studio_service.build_view_status(view_id=results[0]["viewId"], config=config),
        },
    )


def handle_cover_studio_restore(
    handler: Any,
    *,
    store_lock: Any,
    read_store: Callable[[], dict[str, Any]],
    write_store: Callable[[dict[str, Any]], None],
    apply_emby_env_overrides: Callable[[Any], dict[str, Any]],
    normalize_cover_studio_config: Callable[[Any], dict[str, Any]],
    build_emby_service: Callable[[dict[str, Any]], Any],
    cover_studio_service: Any,
) -> None:
    payload = handler._read_json_body()
    if payload is None:
        return
    requested_view_ids = _request_view_ids(payload)
    if not requested_view_ids:
        handler._send_json(400, {"error": "缺少 viewId"})
        return
    with store_lock:
        store = read_store()
        emby_config = apply_emby_env_overrides(store.get("embyConfig"))
        config = normalize_cover_studio_config(store.get("coverStudioConfig"))
    if not _require_emby_ready(handler, emby_config):
        return
    emby_service = build_emby_service(emby_config)
    views = emby_service.fetch_user_views()
    results: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for view_id in requested_view_ids:
        target = _find_cover_view(views, view_id)
        resolved_view_id = str((target or {}).get("id") or view_id).strip()
        upload_target_id = str((target or {}).get("uploadTargetId") or resolved_view_id).strip() or resolved_view_id
        try:
            result = cover_studio_service.restore_backup(
                config=config,
                view_id=resolved_view_id,
                upload_view_id=upload_target_id,
                emby_service=emby_service,
            )
        except Exception as err:
            failures.append({"viewId": resolved_view_id, "error": str(err)})
            continue
        results.append({"viewId": resolved_view_id, "result": result})
    if not results:
        handler._send_json(422, {"error": failures[0]["error"] if failures else "未能恢复封面", "failures": failures})
        return
    with store_lock:
        store = read_store()
        store["coverStudioConfig"] = config
        write_store(store)
    handler._send_json(
        200,
        {
            "ok": True,
            "result": results[0]["result"],
            "results": results,
            "failures": failures,
            "status": cover_studio_service.build_view_status(view_id=results[0]["viewId"], config=config),
        },
    )


def _require_emby_ready(handler: Any, emby_config: dict[str, Any]) -> bool:
    if not str(emby_config.get("serverUrl") or "").strip() or not str(emby_config.get("apiKey") or "").strip():
        handler._send_json(400, {"error": "请先在媒体库配置中填写 Emby 地址和 API Key。"})
        return False
    return True


def _find_cover_view(views: list[dict[str, Any]], view_id: str) -> dict[str, Any] | None:
    safe_view_id = str(view_id or "").strip()
    if not safe_view_id:
        return None
    for row in views:
        if not isinstance(row, dict):
            continue
        candidates = {
            str(row.get("id") or "").strip(),
            str(row.get("browseId") or "").strip(),
            str(row.get("uploadTargetId") or "").strip(),
            str(row.get("userViewId") or "").strip(),
            str(row.get("virtualFolderId") or "").strip(),
        }
        if safe_view_id in candidates:
            return row
    return None


def _request_view_ids(payload: dict[str, Any]) -> list[str]:
    raw_values = payload.get("viewIds") if isinstance(payload.get("viewIds"), list) else [payload.get("viewId")]
    rows: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        view_id = str(raw_value or "").strip()
        if not view_id or view_id in seen:
            continue
        seen.add(view_id)
        rows.append(view_id)
    return rows[:30]


def _request_apply_items(payload: dict[str, Any]) -> list[dict[str, str]]:
    raw_rows = payload.get("items") if isinstance(payload.get("items"), list) else [payload]
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw_row in raw_rows:
        item = raw_row if isinstance(raw_row, dict) else {}
        view_id = str(item.get("viewId") or "").strip()
        preview_token = str(item.get("previewToken") or "").strip()
        if not view_id or not preview_token or view_id in seen:
            continue
        seen.add(view_id)
        rows.append({"viewId": view_id, "previewToken": preview_token})
    return rows[:30]


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
