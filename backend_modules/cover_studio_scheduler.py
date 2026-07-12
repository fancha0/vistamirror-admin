from __future__ import annotations

from datetime import datetime
import threading
from typing import Any, Callable

from backend_modules.cover_studio_service import is_valid_cover_studio_cron, normalize_cover_studio_font_key


class CoverStudioScheduler:
    """Runs one independent automatic-cover plan per Emby media library."""

    def __init__(
        self,
        *,
        stop_event: threading.Event,
        store_lock: Any,
        read_store: Callable[[], dict[str, Any]],
        write_store: Callable[[dict[str, Any]], None],
        normalize_config: Callable[[Any], dict[str, Any]],
        apply_emby_config: Callable[[Any], dict[str, Any]],
        build_emby_service: Callable[[dict[str, Any]], Any],
        cover_service: Any,
        event_logger: Callable[..., None],
    ) -> None:
        self.stop_event = stop_event
        self.store_lock = store_lock
        self.read_store = read_store
        self.write_store = write_store
        self.normalize_config = normalize_config
        self.apply_emby_config = apply_emby_config
        self.build_emby_service = build_emby_service
        self.cover_service = cover_service
        self.event_logger = event_logger
        self._run_lock = threading.Lock()
        self._last_checked_minute = ""
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run_loop, name="cover-studio-scheduler", daemon=True)
        self._thread.start()

    def _run_loop(self) -> None:
        while not self.stop_event.wait(15):
            now = datetime.now().replace(second=0, microsecond=0)
            minute_key = now.isoformat(timespec="minutes")
            if minute_key == self._last_checked_minute:
                continue
            self._last_checked_minute = minute_key
            self.run_once(trigger="scheduled", now=now)

    def run_once(
        self,
        *,
        trigger: str = "manual",
        plan_id: str = "",
        force: bool = False,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        if not self._run_lock.acquire(blocking=False):
            return {"ok": False, "status": "running", "message": "封面计划任务正在执行。", "results": []}
        try:
            return self._run_once_locked(
                trigger=trigger,
                plan_id=str(plan_id or "").strip(),
                force=bool(force),
                now=(now or datetime.now()).replace(second=0, microsecond=0),
            )
        finally:
            self._run_lock.release()

    def _run_once_locked(self, *, trigger: str, plan_id: str, force: bool, now: datetime) -> dict[str, Any]:
        with self.store_lock:
            store = self.read_store()
            config = self.normalize_config(store.get("coverStudioConfig"))
            emby_config = self.apply_emby_config(store.get("embyConfig"))
        plans = [dict(row) for row in config.get("schedules", []) if isinstance(row, dict)]
        selected = [row for row in plans if not plan_id or str(row.get("id") or "") == plan_id]
        if plan_id and not selected:
            return {"ok": False, "status": "error", "message": "未找到封面计划。", "results": []}
        if trigger == "scheduled":
            selected = [
                row for row in selected
                if row.get("enabled") and cron_matches(now, str(row.get("cron") or ""))
            ]
        if not selected:
            return {"ok": True, "status": "idle", "message": "当前没有到期的封面计划。", "results": []}
        if not str(emby_config.get("serverUrl") or "").strip() or not str(emby_config.get("apiKey") or "").strip():
            return self._persist_results(config, plans, selected, [
                _plan_result(row, "error", "Emby 地址或 API Key 未配置。", now)
                for row in selected
            ], trigger=trigger)

        service = self.build_emby_service(emby_config)
        try:
            views = service.fetch_user_views()
        except Exception as err:
            return self._persist_results(config, plans, selected, [
                _plan_result(row, "error", f"读取 Emby 媒体库失败：{err}", now)
                for row in selected
            ], trigger=trigger)

        results: list[dict[str, Any]] = []
        for plan in selected:
            results.append(self._run_plan(plan, config=config, views=views, service=service, now=now, force=force))
        return self._persist_results(config, plans, selected, results, trigger=trigger)

    def _run_plan(
        self,
        plan: dict[str, Any],
        *,
        config: dict[str, Any],
        views: list[dict[str, Any]],
        service: Any,
        now: datetime,
        force: bool,
    ) -> dict[str, Any]:
        target = _find_view(views, str(plan.get("viewId") or ""))
        if not target:
            return _plan_result(plan, "error", "未找到对应的 Emby 媒体库。", now)
        resolved_view_id = str(target.get("id") or plan.get("viewId") or "").strip()
        browse_view_id = str(target.get("browseId") or resolved_view_id).strip() or resolved_view_id
        upload_view_id = str(target.get("uploadTargetId") or resolved_view_id).strip() or resolved_view_id
        view_name = str(target.get("name") or plan.get("viewName") or resolved_view_id).strip()
        try:
            fingerprint = _normalize_fingerprint(service.fetch_view_fingerprint(view_id=browse_view_id))
        except Exception as err:
            return _plan_result(plan, "error", f"检查媒体库失败：{err}", now, view_name=view_name)
        previous = _normalize_fingerprint(plan.get("fingerprint"))
        if not previous["latestItemId"] and not force:
            return _plan_result(
                plan,
                "initialized",
                "已建立媒体库基线；后续检测到新影视时自动更新封面。",
                now,
                view_name=view_name,
                fingerprint=fingerprint,
            )
        if not force and fingerprint == previous:
            return _plan_result(plan, "no_change", "未发现新影视，保留当前封面。", now, view_name=view_name, fingerprint=fingerprint)

        template = plan.get("template") if isinstance(plan.get("template"), dict) else {}
        try:
            preview = self.cover_service.generate_preview(
                view=target,
                items=service.fetch_view_items(view_id=browse_view_id, pick_mode=str(template.get("pickMode") or "random")),
                template_key=str(template.get("templateKey") or "fan_spread"),
                font_key=normalize_cover_studio_font_key(template.get("fontKey")),
                title_text=str(template.get("titleText") or "").strip() or view_name,
                subtitle_text=str(template.get("subtitleText") or "").strip(),
                title_font_size=template.get("titleFontSize"),
                subtitle_font_size=template.get("subtitleFontSize"),
                title_align=str(template.get("titleAlign") or "left"),
                overlay_strength=0,
                poster_count=template.get("posterCount"),
                accent_tone=str(template.get("accentTone") or "blue"),
                poster_rotation=template.get("posterRotation"),
                title_y_offset=template.get("titleYOffset"),
                emby_service=service,
            )
            self.cover_service.backup_and_apply(
                config=config,
                view_id=resolved_view_id,
                upload_view_id=upload_view_id,
                preview_token=preview.token,
                emby_service=service,
            )
        except Exception as err:
            return _plan_result(plan, "error", f"生成或应用封面失败：{err}", now, view_name=view_name, fingerprint=previous)
        return _plan_result(
            plan,
            "success",
            "检测到新影视，已更新 Primary 封面。" if not force else "已手动更新 Primary 封面。",
            now,
            view_name=view_name,
            fingerprint=fingerprint,
            updated=True,
        )

    def _persist_results(
        self,
        config: dict[str, Any],
        plans: list[dict[str, Any]],
        selected: list[dict[str, Any]],
        results: list[dict[str, Any]],
        *,
        trigger: str,
    ) -> dict[str, Any]:
        by_id = {str(result.get("id") or ""): result for result in results}
        updated_plans: list[dict[str, Any]] = []
        for plan in plans:
            result = by_id.get(str(plan.get("id") or ""))
            updated_plans.append(result.get("plan") if isinstance(result, dict) and isinstance(result.get("plan"), dict) else plan)
        config["schedules"] = updated_plans
        with self.store_lock:
            store = self.read_store()
            current = self.normalize_config(store.get("coverStudioConfig"))
            current["backups"] = config.get("backups") if isinstance(config.get("backups"), dict) else current.get("backups", {})
            current["schedules"] = updated_plans
            store["coverStudioConfig"] = current
            self.write_store(store)

        updated_count = sum(1 for result in results if result.get("status") == "success")
        error_count = sum(1 for result in results if result.get("status") == "error")
        if results:
            message = f"封面计划已检查 {len(results)} 个媒体库，更新 {updated_count} 个"
            if error_count:
                message += f"，失败 {error_count} 个"
            message += "。"
            self.event_logger(
                level="warning" if error_count else "info",
                module="cover_studio",
                action="cover_studio_schedule_run",
                message=message,
                detail={"trigger": trigger, "results": results},
            )
        return {"ok": error_count == 0, "status": "success" if updated_count else "checked", "message": message if results else "当前没有到期的封面计划。", "results": results}


def _plan_result(
    plan: dict[str, Any],
    status: str,
    message: str,
    now: datetime,
    *,
    view_name: str = "",
    fingerprint: dict[str, Any] | None = None,
    updated: bool = False,
) -> dict[str, Any]:
    next_plan = dict(plan)
    next_plan["viewName"] = view_name or str(plan.get("viewName") or "")
    next_plan["lastCheckedAt"] = now.isoformat(timespec="seconds")
    next_plan["lastStatus"] = status
    next_plan["lastMessage"] = message
    if fingerprint is not None:
        next_plan["fingerprint"] = fingerprint
    if status == "initialized":
        next_plan["initializedAt"] = now.isoformat(timespec="seconds")
    if updated:
        next_plan["lastUpdatedAt"] = now.isoformat(timespec="seconds")
    return {"id": str(plan.get("id") or ""), "viewId": str(plan.get("viewId") or ""), "status": status, "message": message, "plan": next_plan}


def _normalize_fingerprint(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    try:
        item_count = max(0, int(source.get("itemCount") or 0))
    except (TypeError, ValueError):
        item_count = 0
    return {
        "itemCount": item_count,
        "latestItemId": str(source.get("latestItemId") or "").strip(),
        "latestCreatedAt": str(source.get("latestCreatedAt") or "").strip(),
    }


def cron_matches(now: datetime, expression: str) -> bool:
    if not is_valid_cover_studio_cron(expression):
        return False
    minute, hour, day, month, weekday = str(expression).split()
    return all(
        (
            _field_matches(now.minute, minute, 0, 59),
            _field_matches(now.hour, hour, 0, 23),
            _field_matches(now.day, day, 1, 31),
            _field_matches(now.month, month, 1, 12),
            _field_matches((now.weekday() + 1) % 7, weekday, 0, 6),
        )
    )


def _field_matches(value: int, expression: str, minimum: int, maximum: int) -> bool:
    for raw_part in str(expression).split(","):
        base, separator, step_text = raw_part.partition("/")
        step = int(step_text) if separator else 1
        if base == "*":
            start, end = minimum, maximum
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            start, end = int(start_text), int(end_text)
        else:
            start = end = int(base)
        if start <= value <= end and (value - start) % step == 0:
            return True
    return False


def _find_view(views: list[dict[str, Any]], view_id: str) -> dict[str, Any] | None:
    wanted = str(view_id or "").strip()
    for view in views:
        if not isinstance(view, dict):
            continue
        candidates = {
            str(view.get("id") or "").strip(),
            str(view.get("browseId") or "").strip(),
            str(view.get("uploadTargetId") or "").strip(),
            str(view.get("userViewId") or "").strip(),
            str(view.get("virtualFolderId") or "").strip(),
        }
        if wanted in candidates:
            return view
    return None
