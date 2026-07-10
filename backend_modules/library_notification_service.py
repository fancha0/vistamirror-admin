from __future__ import annotations

from typing import Any

from .notification_platform import any_route_enabled


class LibraryNotificationService:
    def __init__(self, host: Any) -> None:
        self.host = host

    def notify_item_unlocked(
        self,
        *,
        item_id: str,
        payload: dict[str, Any] | None = None,
        source: str = "webhook",
        state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        safe_item_id = str(item_id or "").strip()
        if not safe_item_id:
            return {"ok": False, "status": "missing_item_id"}
        notification, _bot, _emby = self.host._library_notification_bundle()
        if not bool(notification.get("enabled", True)):
            return {"ok": True, "status": "disabled"}
        if not any_route_enabled(notification, ("library.single", "library.grouped")):
            return {"ok": True, "status": "disabled"}

        state = state if isinstance(state, dict) else self.host._read_library_notification_state()
        if safe_item_id in state["seen"]:
            self.host._log_project_event(
                level="info",
                module="webhook",
                action="library_notification_duplicate_skipped",
                message="重复入库资源通知已跳过。",
                detail={"itemId": safe_item_id, "source": source},
            )
            return {"ok": True, "status": "duplicate"}

        try:
            detail = self.host._fetch_library_item_detail(safe_item_id)
        except Exception as err:
            detail = {}
            self.host._log_project_event(
                level="warning",
                module="webhook",
                action="library_notification_detail_fallback",
                message="入库资源详情读取失败，使用事件基础资料继续发送。",
                detail={"itemId": safe_item_id, "source": source, "error": str(err)},
            )
        if isinstance(payload, dict):
            merged = dict(payload)
            merged.update(detail)
            detail = merged
        item_type = str(detail.get("Type") or "").strip().lower()
        if item_type not in {"movie", "episode"}:
            return {"ok": True, "status": "filtered", "itemType": item_type}
        if item_type == "episode" and any_route_enabled(notification, ("library.grouped",)):
            buffered = self.host._buffer_library_episode_group(state=state, payload=detail, source=source)
            if str(buffered.get("status") or "") != "single_fallback":
                return buffered
        return self.send_single_notification_unlocked(
            item_id=safe_item_id,
            detail=detail,
            source=source,
            state=state,
        )

    def send_single_notification_unlocked(
        self,
        *,
        item_id: str,
        detail: dict[str, Any],
        source: str,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        safe_item_id = str(item_id or "").strip()
        notification, _bot, _emby = self.host._library_notification_bundle()
        item_type = str(detail.get("Type") or "").strip().lower()
        series_detail: dict[str, Any] = {}
        if item_type == "episode":
            series_id = str(detail.get("SeriesId") or "").strip()
            if series_id:
                try:
                    series_detail = self.host._fetch_library_item_detail(series_id)
                except Exception as err:
                    self.host._log_project_event(
                        level="warning",
                        module="webhook",
                        action="library_notification_series_detail_fallback",
                        message="所属剧集资料读取失败，继续发送单集通知。",
                        detail={"itemId": safe_item_id, "seriesId": series_id, "source": source, "error": str(err)},
                    )
        context = self.host._format_library_notification_context(detail, series_detail)
        delivery_context: dict[str, Any] = {}
        if self.host._library_channel_enabled(notification, "telegram", "library.single"):
            photo = self.resolve_photo(
                image_item_ids=self.host._library_notification_image_ids(detail, series_detail),
                item_id=safe_item_id,
                source=source,
            )
            if photo:
                delivery_context["telegram"] = photo
        dispatch_result = self.host._notification_dispatcher().dispatch(
            config=notification,
            event={
                "eventKey": "library.single",
                "payload": context,
                "channelContext": delivery_context,
                "source": source,
                "traceId": safe_item_id,
            },
        )
        sent_results = [row for row in dispatch_result.get("results", []) if isinstance(row, dict) and row.get("status") == "sent"]
        if not sent_results:
            skipped = [row for row in dispatch_result.get("results", []) if isinstance(row, dict) and row.get("status") == "skipped"]
            if skipped:
                first_reason = str(skipped[0].get("reason") or "not_configured")
                return {"ok": True, "status": first_reason}
            return {"ok": False, "status": "send_failed"}
        state["active"] = True
        state["seen"][safe_item_id] = self.host._library_notification_now_iso()
        self.host._write_library_notification_state(state)
        self.host._log_project_event(
            level="info",
            module="webhook",
            action="library_notification_sent",
            message="Telegram 新入库海报通知已发送。",
            detail={
                "itemId": safe_item_id,
                "itemType": item_type,
                "source": source,
                "channels": [str(row.get("channel") or "") for row in sent_results],
                "photo": any(str(row.get("mode") or "").startswith("photo") for row in sent_results),
                "detailSource": str(detail.get("_detailSource") or "event_payload"),
                "seriesDetailSource": str(series_detail.get("_detailSource") or ""),
            },
        )
        return {
            "ok": True,
            "status": "sent",
            "photo": any(str(row.get("mode") or "").startswith("photo") for row in sent_results),
            "itemType": item_type,
            "channels": [str(row.get("channel") or "") for row in sent_results],
        }

    def send_group_notification_unlocked(self, group: dict[str, Any]) -> None:
        notification, _bot, _emby = self.host._library_notification_bundle()
        if not any_route_enabled(notification, ("library.grouped",)):
            raise RuntimeError("library_grouped_disabled")

        items = [dict(row) for row in group.get("items", []) if isinstance(row, dict) and str(row.get("Id") or "").strip()]
        if not items:
            return
        series_id = str(group.get("seriesId") or "").strip()
        sources = sorted({str(value).strip() for value in group.get("sources", []) if str(value).strip()})
        resolved_items = [self.host._resolve_library_group_item(row) for row in items]
        if len(resolved_items) <= 1:
            row = resolved_items[0]
            result = self.send_single_notification_unlocked(
                item_id=str(row.get("Id") or "").strip(),
                detail=row,
                source="+".join(sources) or "group",
                state=self.host._read_library_notification_state(),
            )
            if not bool(result.get("ok")):
                raise RuntimeError(f"group_single_fallback_failed:{result.get('status')}")
            return

        series_detail = self.host._fetch_library_item_detail(series_id) if series_id else {}
        context = self.host._format_library_group_notification_context(group=group, items=resolved_items, series_detail=series_detail)
        delivery_context: dict[str, Any] = {}
        image_item_ids = self.host._library_notification_image_ids(
            {"Type": "Episode", "SeriesId": series_id, "PrimaryImageItemId": series_detail.get("PrimaryImageItemId")},
            series_detail,
        )
        if self.host._library_channel_enabled(notification, "telegram", "library.grouped"):
            photo = self.resolve_photo(
                image_item_ids=image_item_ids,
                item_id=series_id or ",".join(str(row.get("Id") or "") for row in resolved_items),
                source="+".join(sources) or "group",
            )
            if photo:
                delivery_context["telegram"] = photo
        item_ids = [str(row.get("Id") or "").strip() for row in resolved_items if str(row.get("Id") or "").strip()]
        dispatch_result = self.host._notification_dispatcher().dispatch(
            config=notification,
            event={
                "eventKey": "library.grouped",
                "payload": context,
                "channelContext": delivery_context,
                "source": "+".join(sources) or "group",
                "traceId": series_id or ",".join(item_ids),
            },
        )
        sent_results = [row for row in dispatch_result.get("results", []) if isinstance(row, dict) and row.get("status") == "sent"]
        if not sent_results:
            raise RuntimeError("library_group_dispatch_failed")
        state = self.host._read_library_notification_state()
        state["active"] = True
        sent_at = self.host._library_notification_now_iso()
        for item_id in item_ids:
            state["seen"][item_id] = sent_at
        self.host._write_library_notification_state(state)
        range_text = self.host._library_group_episode_range_text(resolved_items)
        self.host._log_project_event(
            level="info",
            module="webhook",
            action="library_notification_group_sent",
            message="Telegram 剧集合并入库通知已发送。",
            detail={
                "seriesId": series_id,
                "seriesName": str(series_detail.get("Name") or group.get("seriesName") or ""),
                "episodeCount": len(item_ids),
                "seasonCount": self.host._library_group_season_count(resolved_items),
                "rangeText": range_text,
                "photo": any(str(row.get("mode") or "").startswith("photo") for row in sent_results),
                "channels": [str(row.get("channel") or "") for row in sent_results],
                "source": "+".join(sources) or "group",
            },
        )

    def resolve_photo(
        self,
        *,
        image_item_ids: list[str],
        item_id: str,
        source: str,
    ) -> dict[str, Any]:
        for image_item_id in image_item_ids:
            try:
                photo_bytes = self.host._fetch_emby_primary_image(image_item_id)
                if not photo_bytes:
                    continue
                return {
                    "photoBytes": photo_bytes,
                    "filename": f"{image_item_id}.jpg",
                    "contentType": "image/jpeg",
                }
            except Exception as err:
                self.host._log_project_event(
                    level="warning",
                    module="webhook",
                    action="library_notification_photo_fallback",
                    message="入库海报读取失败，继续发送文字通知。",
                    detail={"itemId": item_id, "source": source, "error": str(err)},
                )
        return {}
