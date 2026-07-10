from __future__ import annotations

from typing import Any, Callable

from .ip_locator import build_ip_display
from .message_formatter import (
    compose_playback_message,
    detect_event_time,
    format_episode_display_tag,
    format_episode_tag,
    format_playback_media_spec,
    format_playback_strategy,
    format_playback_title_line,
    format_playback_device_name,
    normalize_content_type,
    playback_state_meta,
    ticks_to_seconds,
)


def build_playback_image_candidates(
    *,
    payload: dict[str, Any],
    item_detail: dict[str, Any],
    series_detail: dict[str, Any] | None = None,
    parent_detail: dict[str, Any] | None = None,
    item_id: str,
    pick_first_value: Callable[[dict[str, Any], list[tuple[str, ...]]], str],
) -> list[dict[str, str]]:
    detail_map = item_detail if isinstance(item_detail, dict) else {}
    series_map = series_detail if isinstance(series_detail, dict) else {}
    parent_map = parent_detail if isinstance(parent_detail, dict) else {}
    image_candidates: list[dict[str, str]] = []
    seen_image_keys: set[tuple[str, str]] = set()

    def first_value(detail_value: Any, *payload_paths: tuple[str, ...]) -> str:
        safe_detail = str(detail_value or "").strip()
        if safe_detail:
            return safe_detail
        return str(pick_first_value(payload, list(payload_paths)) or "").strip()

    def first_image_tag(detail_value: Any, *payload_paths: tuple[str, ...]) -> str:
        safe_detail = str(detail_value or "").strip()
        if safe_detail:
            return safe_detail
        for path in payload_paths:
            current: Any = payload
            found = True
            for key in path:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    found = False
                    break
            if found and current not in (None, ""):
                return str(current).strip()
        return ""

    def push_candidate(raw_item_id: Any, raw_tag: Any = "", *, source: str = "") -> None:
        safe_item_id = str(raw_item_id or "").strip()
        safe_tag = str(raw_tag or "").strip()
        if not safe_item_id:
            return
        dedupe_key = (safe_item_id, safe_tag)
        if dedupe_key in seen_image_keys:
            return
        seen_image_keys.add(dedupe_key)
        image_candidates.append(
            {
                "itemId": safe_item_id,
                "tag": safe_tag,
                "source": str(source or "").strip(),
            }
        )

    detail_type = str(detail_map.get("Type") or "").strip().lower()
    resolved_item_id = str(item_id or detail_map.get("Id") or "").strip()
    resolved_item_tag = first_image_tag(
        detail_map.get("ImageTags", {}).get("Primary") if isinstance(detail_map.get("ImageTags"), dict) else "",
        ("ImageTags", "Primary"),
        ("Item", "ImageTags", "Primary"),
        ("item", "imageTags", "Primary"),
        ("NowPlayingItem", "ImageTags", "Primary"),
        ("nowPlayingItem", "imageTags", "Primary"),
    )
    resolved_series_id = first_value(
        detail_map.get("SeriesId"),
        ("SeriesId",),
        ("seriesId",),
        ("Item", "SeriesId"),
        ("item", "seriesId"),
        ("NowPlayingItem", "SeriesId"),
        ("nowPlayingItem", "seriesId"),
    )
    resolved_series_tag = first_image_tag(
        (series_map.get("ImageTags", {}) or {}).get("Primary") if isinstance(series_map.get("ImageTags"), dict) else detail_map.get("SeriesPrimaryImageTag"),
        ("SeriesPrimaryImageTag",),
        ("seriesPrimaryImageTag",),
        ("Item", "SeriesPrimaryImageTag"),
        ("item", "seriesPrimaryImageTag"),
        ("NowPlayingItem", "SeriesPrimaryImageTag"),
        ("nowPlayingItem", "seriesPrimaryImageTag"),
    )
    resolved_parent_id = first_value(
        detail_map.get("ParentId"),
        ("ParentId",),
        ("parentId",),
        ("Item", "ParentId"),
        ("item", "parentId"),
        ("NowPlayingItem", "ParentId"),
        ("nowPlayingItem", "parentId"),
    )
    resolved_parent_tag = first_image_tag(
        (parent_map.get("ImageTags", {}) or {}).get("Primary") if isinstance(parent_map.get("ImageTags"), dict) else detail_map.get("ParentPrimaryImageTag"),
        ("ParentPrimaryImageTag",),
        ("parentPrimaryImageTag",),
        ("Item", "ParentPrimaryImageTag"),
        ("item", "parentPrimaryImageTag"),
        ("NowPlayingItem", "ParentPrimaryImageTag"),
        ("nowPlayingItem", "parentPrimaryImageTag"),
    )
    resolved_series_detail_id = str(series_map.get("Id") or resolved_series_id).strip()
    resolved_parent_detail_id = str(parent_map.get("Id") or resolved_parent_id).strip()

    episode_like = detail_type == "episode"
    if not episode_like:
        episode_like = bool(
            resolved_series_detail_id
            and resolved_series_detail_id != resolved_item_id
            and detail_type in {"", "video"}
            and (
                bool(resolved_parent_detail_id)
                or bool(resolved_series_tag)
                or bool(resolved_parent_tag)
            )
        )

    if episode_like:
        push_candidate(resolved_series_detail_id, resolved_series_tag, source="series_primary")
        push_candidate(resolved_parent_detail_id, resolved_parent_tag, source="parent_primary")
        push_candidate(resolved_item_id, resolved_item_tag, source="item_primary")
    else:
        push_candidate(resolved_item_id, resolved_item_tag, source="item_primary")
        push_candidate(resolved_series_detail_id, resolved_series_tag, source="series_primary")
        push_candidate(resolved_parent_detail_id, resolved_parent_tag, source="parent_primary")

    push_candidate(detail_map.get("PrimaryImageItemId"), "", source="primary_image_item")
    return image_candidates


class PlaybackNotificationEventFactory:
    def __init__(
        self,
        *,
        fetch_session_detail: Callable[..., dict[str, Any]],
        extract_item_id: Callable[[dict[str, Any]], str],
        fetch_item_detail: Callable[..., dict[str, Any]],
        pick_first_value: Callable[[dict[str, Any], list[tuple[str, ...]]], str],
        safe_float: Callable[[Any], float | None],
        build_item_urls: Callable[..., tuple[str, str]],
        format_hms: Callable[[int], str],
        shorten_caption: Callable[[str], str],
        shorten_overview: Callable[[str], str],
    ) -> None:
        self._fetch_session_detail = fetch_session_detail
        self._extract_item_id = extract_item_id
        self._fetch_item_detail = fetch_item_detail
        self._pick_first_value = pick_first_value
        self._safe_float = safe_float
        self._build_item_urls = build_item_urls
        self._format_hms = format_hms
        self._shorten_caption = shorten_caption
        self._shorten_overview = shorten_overview

    @staticmethod
    def _merge_playback_detail(base: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
        merged = dict(base) if isinstance(base, dict) else {}
        if not isinstance(fallback, dict):
            return merged
        for key, value in fallback.items():
            if key not in merged or merged.get(key) in (None, "", {}):
                merged[key] = value
        if (
            isinstance(merged.get("ImageTags"), dict)
            and isinstance(fallback.get("ImageTags"), dict)
            and not merged.get("ImageTags")
        ):
            merged["ImageTags"] = dict(fallback.get("ImageTags") or {})
        return merged

    def _build_playback_device_display(self, payload: dict[str, Any]) -> str:
        client_name = self._pick_first_value(
            payload,
            [("Client",), ("client",), ("Session", "Client"), ("session", "client")],
        )
        device_name = self._pick_first_value(
            payload,
            [("DeviceName",), ("deviceName",), ("Session", "DeviceName"), ("session", "deviceName")],
        )
        return format_playback_device_name(client_name, device_name)

    @staticmethod
    def _pick_first_positive_number(payload: dict[str, Any], paths: list[tuple[str, ...]]) -> Any:
        fallback: Any = ""
        for path in paths:
            current: Any = payload
            found = True
            for key in path:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    found = False
                    break
            if not found or current in (None, ""):
                continue
            if fallback in ("", None):
                fallback = current
            try:
                if int(current) > 0:
                    return current
            except (TypeError, ValueError):
                continue
        return fallback

    def build(
        self,
        payload: dict[str, Any],
        *,
        action: str,
        event_name: str,
        emby_config: dict[str, Any],
        bot_config: dict[str, Any],
        session_id: str = "",
    ) -> dict[str, Any]:
        session_detail = self._fetch_session_detail(emby_config=emby_config, session_id=session_id)
        if session_detail:
            merged_payload = dict(payload)
            for key in ("UserName", "DeviceName", "Client", "RemoteEndPoint"):
                if not str(merged_payload.get(key) or "").strip() and str(session_detail.get(key) or "").strip():
                    merged_payload[key] = session_detail.get(key)
            if not isinstance(merged_payload.get("Session"), dict):
                merged_payload["Session"] = {}
            if isinstance(merged_payload.get("Session"), dict):
                session_map = dict(merged_payload["Session"])
                for key in ("UserName", "DeviceName", "Client", "RemoteEndPoint"):
                    if not str(session_map.get(key) or "").strip() and str(session_detail.get(key) or "").strip():
                        session_map[key] = session_detail.get(key)
                merged_payload["Session"] = session_map
            payload = merged_payload

        item_id = self._extract_item_id(payload)
        item_detail = self._fetch_item_detail(emby_config=emby_config, item_id=item_id)
        now_playing_item = session_detail.get("NowPlayingItem") if isinstance(session_detail.get("NowPlayingItem"), dict) else {}
        payload_item = payload.get("NowPlayingItem") if isinstance(payload.get("NowPlayingItem"), dict) else {}
        if not payload_item and isinstance(payload.get("Item"), dict):
            payload_item = dict(payload.get("Item") or {})
        item_detail = self._merge_playback_detail(item_detail, now_playing_item)
        item_detail = self._merge_playback_detail(item_detail, payload_item)
        if item_id and not str(item_detail.get("Id") or "").strip():
            item_detail["Id"] = item_id

        series_detail: dict[str, Any] = {}
        parent_detail: dict[str, Any] = {}
        if isinstance(item_detail, dict):
            series_id = str(item_detail.get("SeriesId") or "").strip()
            parent_id = str(item_detail.get("ParentId") or "").strip()
            detail_type = str(item_detail.get("Type") or "").strip().lower()
            episode_like = detail_type in {"episode", "season"} or bool(
                series_id
                and (
                    parent_id
                    or str(item_detail.get("SeriesName") or "").strip()
                    or item_detail.get("ParentIndexNumber") not in (None, "")
                    or item_detail.get("IndexNumber") not in (None, "")
                )
            )
            if series_id and episode_like:
                series_detail = self._fetch_item_detail(emby_config=emby_config, item_id=series_id)
            if parent_id and episode_like:
                parent_detail = self._fetch_item_detail(emby_config=emby_config, item_id=parent_id)

        user_name = self._pick_first_value(
            payload,
            [("UserName",), ("userName",), ("User", "Name"), ("user", "name"), ("Session", "UserName"), ("session", "userName")],
        ) or "未知用户"
        item_name = self._pick_first_value(
            payload,
            [
                ("ItemName",),
                ("itemName",),
                ("NowPlayingItem", "Name"),
                ("nowPlayingItem", "name"),
                ("Item", "Name"),
                ("item", "name"),
                ("Name",),
            ],
        )
        series_name = self._pick_first_value(
            payload,
            [("SeriesName",), ("seriesName",), ("NowPlayingItem", "SeriesName"), ("nowPlayingItem", "seriesName"), ("Item", "SeriesName")],
        ) or str(item_detail.get("SeriesName") or "").strip()
        if not item_name:
            item_name = str(item_detail.get("Name") or "").strip() or "未知内容"

        episode_tag = format_episode_tag(payload, item_detail)
        content_type = self._pick_first_value(payload, [("ItemType",), ("itemType",), ("Type",), ("type",)])
        if not content_type:
            content_type = str(item_detail.get("Type") or "").strip()
        if not content_type:
            if series_name or episode_tag:
                content_type = "episode"
            elif str(item_detail.get("MediaType") or "").strip().lower() == "audio":
                content_type = "audio"
            elif str(item_detail.get("MediaType") or "").strip().lower() == "video":
                content_type = "movie"
        content_type = normalize_content_type(content_type)

        rating = self._safe_float(
            self._pick_first_value(payload, [("CommunityRating",), ("communityRating",), ("Item", "CommunityRating"), ("item", "communityRating")])
            or item_detail.get("CommunityRating")
        )
        rating_text = f"{rating:.1f}/10" if rating is not None and rating > 0 else ""

        position_ticks = self._pick_first_positive_number(
            payload,
            [
                ("PositionTicks",),
                ("positionTicks",),
                ("PlaybackPositionTicks",),
                ("StopPositionTicks",),
                ("stopPositionTicks",),
                ("LastPositionTicks",),
                ("lastPositionTicks",),
                ("PlaybackInfo", "PositionTicks"),
                ("playbackInfo", "positionTicks"),
                ("PlaybackInfo", "StopPositionTicks"),
                ("playbackInfo", "stopPositionTicks"),
                ("PlaybackInfo", "LastPositionTicks"),
                ("playbackInfo", "lastPositionTicks"),
                ("Session", "PlayState", "PositionTicks"),
                ("session", "playState", "positionTicks"),
            ],
        )
        runtime_ticks = self._pick_first_positive_number(
            payload,
            [
                ("RunTimeTicks",),
                ("runTimeTicks",),
                ("Item", "RunTimeTicks"),
                ("item", "runTimeTicks"),
                ("NowPlayingItem", "RunTimeTicks"),
                ("nowPlayingItem", "runTimeTicks"),
                ("Session", "NowPlayingItem", "RunTimeTicks"),
                ("session", "nowPlayingItem", "runTimeTicks"),
                ("PlaybackInfo", "MediaSource", "RunTimeTicks"),
                ("playbackInfo", "mediaSource", "runTimeTicks"),
                ("PlaybackInfo", "RunTimeTicks"),
                ("playbackInfo", "runTimeTicks"),
            ],
        )
        position_sec = ticks_to_seconds(position_ticks)
        runtime_sec = ticks_to_seconds(runtime_ticks or item_detail.get("RunTimeTicks"))
        percent = ""
        if runtime_sec > 0:
            ratio = max(0.0, min(1.0, float(position_sec) / float(runtime_sec)))
            percent = f"{int(round(ratio * 100))}%"

        device_name = self._build_playback_device_display(payload)
        overview = (
            str(item_detail.get("Overview") or "").strip()
            or self._pick_first_value(payload, [("Overview",), ("overview",), ("Item", "Overview"), ("item", "overview")])
            or ""
        )

        poster_url, detail_url = self._build_item_urls(emby_config=emby_config, item_id=item_id)

        caption = compose_playback_message(
            payload=payload,
            item_detail=item_detail,
            action=action,
            username=user_name,
            series_name=series_name,
            item_name=item_name,
            content_type=content_type,
            rating_text=rating_text,
            position_sec=position_sec,
            runtime_sec=runtime_sec,
            percent_text=percent,
            device_name=device_name,
            overview=overview,
            show_ip=bool(bot_config.get("showIp", True)),
            show_ip_geo=bool(bot_config.get("showIpGeo", True)),
            show_overview=bool(bot_config.get("showOverview", True)),
        )
        if len(caption) > 1000:
            caption = self._shorten_caption(caption)

        state_meta = playback_state_meta(action)
        action_text = state_meta["action_text"]
        episode_display_tag = format_episode_display_tag(payload, item_detail)
        title_body = ""
        if content_type == "剧集":
            title_body = " - ".join(part for part in (series_name, episode_display_tag, item_name) if str(part or "").strip()).strip()
        else:
            title_body = str(item_name or series_name or "").strip()
        meta_parts: list[str] = []
        if rating_text:
            meta_parts.append(f"⭐ 评分：{rating_text}")
        if content_type:
            meta_parts.append(f"📚 类型：{content_type}")
        progress_line = ""
        if runtime_sec > 0:
            progress_line = f"🔄 进度：{self._format_hms(position_sec)} / {self._format_hms(runtime_sec)}"
            if percent:
                progress_line += f" ({percent})"
        ip_display = build_ip_display(
            payload,
            show_ip=bool(bot_config.get("showIp", True)),
            show_geo=bool(bot_config.get("showIpGeo", True)),
        )
        occurred_at = detect_event_time(payload)
        overview_text = self._shorten_overview(str(overview or "").strip()) if str(overview or "").strip() else "暂无简介"
        strategy_text = format_playback_strategy(payload, item_detail)
        media_spec = format_playback_media_spec(payload, item_detail)
        title_line = format_playback_title_line(
            action=action,
            payload=payload,
            item_detail=item_detail,
            content_type=content_type,
            series_name=series_name,
            item_name=item_name,
        )
        image_candidates = build_playback_image_candidates(
            payload=payload,
            item_detail=item_detail,
            series_detail=series_detail,
            parent_detail=parent_detail,
            item_id=item_id,
            pick_first_value=self._pick_first_value,
        )
        template_payload = {
            "headline": title_line,
            "meta_line": " ｜ ".join(meta_parts).strip(),
            "title_line": title_line,
            "user_line": f"🍿 播放用户：{user_name}",
            "playback_method_line": f"📽️ 播放策略：{strategy_text}" if strategy_text else "",
            "media_spec_line": f"🎟️ 媒体规格：{media_spec}" if media_spec else "",
            "rating_line": f"▸ 评分：🌟 {rating_text.replace('/10', ' / 10')}" if rating_text else "▸ 评分：🌟 暂无",
            "content_type_line": f"🎬类型：{content_type}" if content_type else "🎬类型：未知",
            "progress_line": (
                progress_line.replace("🔄 进度：", f"▸ 进度：{state_meta['progress_icon']} ", 1)
                if progress_line
                else ""
            ),
            "ip_line": f"▸ 网络：📍 {ip_display}" if ip_display else "",
            "device_line": f"▸ 设备：📺 {device_name}" if device_name else "",
            "occurred_at": occurred_at,
            "time_line": f"▸ 时间：⏰ {occurred_at}",
            "overview_block": (
                f"📖 剧情简介：{self._shorten_overview(str(overview or '').strip())}"
                if bool(bot_config.get("showOverview", True)) and str(overview or "").strip()
                else ""
            ),
            "overview_line": (
                f"📖 剧情简介：{overview_text}"
                if bool(bot_config.get("showOverview", True))
                else ""
            ),
            "user_name": user_name,
            "title": item_name,
            "action_text": action_text,
            "series_name": series_name,
            "episode_tag": episode_tag,
        }
        return {
            "caption": caption,
            "posterUrl": poster_url,
            "detailUrl": detail_url,
            "itemId": item_id,
            "imageCandidates": image_candidates,
            "eventName": str(event_name or "").strip(),
            "templatePayload": template_payload,
        }
