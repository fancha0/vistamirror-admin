import unittest

import dev_server
from backend_modules.notification_event_factory import PlaybackNotificationEventFactory, build_playback_image_candidates


class PlaybackNotificationEventFactoryTests(unittest.TestCase):
    def build_factory(self):
        return PlaybackNotificationEventFactory(
            fetch_session_detail=lambda **kwargs: {},
            extract_item_id=lambda payload: "item-1",
            fetch_item_detail=lambda **kwargs: {
                "Type": "",
                "MediaType": "Video",
                "SeriesName": "你好1983",
                "Name": "你真的想要重新开始吗?",
                "RunTimeTicks": 45 * 60 * 10_000_000 + 52 * 10_000_000,
            },
            pick_first_value=lambda payload, paths: _pick_first_value(payload, paths),
            safe_float=lambda value: float(value) if value not in (None, "") else None,
            build_item_urls=lambda **kwargs: ("", ""),
            format_hms=lambda seconds: f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}" if seconds >= 3600 else f"{seconds // 60:02d}:{seconds % 60:02d}",
            shorten_caption=lambda text: text,
            shorten_overview=lambda text: text,
        )

    def test_falls_back_to_episode_content_type_for_series_items(self):
        factory = self.build_factory()

        result = factory.build(
            {
                "UserName": "lishiya",
                "SeriesName": "你好1983",
                "ItemName": "你真的想要重新开始吗?",
                "ParentIndexNumber": 1,
                "IndexNumber": 1,
                "PositionTicks": 0,
                "RunTimeTicks": 45 * 60 * 10_000_000 + 52 * 10_000_000,
                "DeviceName": "iPhone",
            },
            action="start",
            event_name="playback.start",
            emby_config={},
            bot_config={"showIp": False, "showIpGeo": False, "showOverview": False},
        )

        self.assertIn("🟢 【正在播放】", result["templatePayload"]["headline"])
        self.assertIn("📚 类型：剧集", result["templatePayload"]["meta_line"])
        self.assertIn("▸ 评分：🌟 暂无", result["templatePayload"]["rating_line"])
        self.assertIn("🎬类型：剧集", result["templatePayload"]["content_type_line"])
        self.assertIn("▸ 时间：⏰", result["templatePayload"]["time_line"])

    def test_combines_client_and_device_name_for_device_line(self):
        factory = self.build_factory()

        result = factory.build(
            {
                "UserName": "lishiya",
                "SeriesName": "遮天",
                "ItemName": "灭杀圣联盟",
                "ParentIndexNumber": 1,
                "IndexNumber": 134,
                "PositionTicks": 0,
                "RunTimeTicks": 45 * 60 * 10_000_000,
                "Client": "SenPlayer",
                "DeviceName": "iPhone11",
            },
            action="start",
            event_name="playback.start",
            emby_config={},
            bot_config={"showIp": False, "showIpGeo": False, "showOverview": False},
        )

        self.assertEqual(result["templatePayload"]["device_line"], "▸ 设备：📺 SenPlayer iPhone11")

    def test_dedupes_repeated_client_and_device_name(self):
        factory = self.build_factory()

        result = factory.build(
            {
                "UserName": "lishiya",
                "SeriesName": "遮天",
                "ItemName": "灭杀圣联盟",
                "ParentIndexNumber": 1,
                "IndexNumber": 134,
                "PositionTicks": 0,
                "RunTimeTicks": 45 * 60 * 10_000_000,
                "Client": "iPhone",
                "DeviceName": "iPhone",
            },
            action="start",
            event_name="playback.start",
            emby_config={},
            bot_config={"showIp": False, "showIpGeo": False, "showOverview": False},
        )

        self.assertEqual(result["templatePayload"]["device_line"], "▸ 设备：📺 iPhone")

    def test_prefers_current_episode_overview_for_episode_notifications(self):
        factory = PlaybackNotificationEventFactory(
            fetch_session_detail=lambda **kwargs: {},
            extract_item_id=lambda payload: "episode-134",
            fetch_item_detail=lambda **kwargs: {
                "Type": "Episode",
                "SeriesName": "遮天",
                "Name": "灭杀圣联盟",
                "ParentIndexNumber": 1,
                "IndexNumber": 134,
                "RunTimeTicks": 25 * 60 * 10_000_000,
                "Overview": "这一集的剧集简介",
            },
            pick_first_value=lambda payload, paths: _pick_first_value(payload, paths),
            safe_float=lambda value: float(value) if value not in (None, "") else None,
            build_item_urls=lambda **kwargs: ("", ""),
            format_hms=lambda seconds: f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}" if seconds >= 3600 else f"{seconds // 60:02d}:{seconds % 60:02d}",
            shorten_caption=lambda text: text,
            shorten_overview=lambda text: text,
        )

        result = factory.build(
            {
                "UserName": "lishiya",
                "SeriesName": "遮天",
                "ItemName": "灭杀圣联盟",
                "Overview": "系列级简介",
                "ParentIndexNumber": 1,
                "IndexNumber": 134,
                "PositionTicks": 0,
                "RunTimeTicks": 25 * 60 * 10_000_000,
                "Client": "SenPlayer",
                "DeviceName": "iPhone11",
            },
            action="start",
            event_name="playback.start",
            emby_config={},
            bot_config={"showIp": False, "showIpGeo": False, "showOverview": True},
        )

        self.assertEqual(result["templatePayload"]["overview_line"], "📖 剧情简介：这一集的剧集简介")
        self.assertEqual(result["templatePayload"]["overview_block"], "📖 剧情简介：这一集的剧集简介")

    def test_uses_payload_episode_overview_when_detail_has_none(self):
        factory = PlaybackNotificationEventFactory(
            fetch_session_detail=lambda **kwargs: {},
            extract_item_id=lambda payload: "episode-18",
            fetch_item_detail=lambda **kwargs: {"Type": "Episode", "Name": "陈异为保护苗靖"},
            pick_first_value=lambda payload, paths: _pick_first_value(payload, paths),
            safe_float=lambda value: float(value) if value not in (None, "") else None,
            build_item_urls=lambda **kwargs: ("", ""),
            format_hms=lambda seconds: f"{seconds // 60:02d}:{seconds % 60:02d}",
            shorten_caption=lambda text: text,
            shorten_overview=lambda text: text,
        )

        result = factory.build(
            {
                "SeriesName": "野狗骨头",
                "Overview": "这是不应被当作单集简介的系列简介",
                "Item": {
                    "Id": "episode-18",
                    "Type": "Episode",
                    "Overview": "陈异为保护苗靖，决定独自面对危机。",
                },
            },
            action="stop",
            event_name="playback.stop",
            emby_config={},
            bot_config={"showIp": False, "showIpGeo": False, "showOverview": True},
        )

        self.assertEqual(result["templatePayload"]["overview_line"], "📖 剧情简介：陈异为保护苗靖，决定独自面对危机。")

    def test_episode_does_not_use_ambiguous_top_level_overview(self):
        factory = PlaybackNotificationEventFactory(
            fetch_session_detail=lambda **kwargs: {},
            extract_item_id=lambda payload: "episode-18",
            fetch_item_detail=lambda **kwargs: {"Type": "Episode", "Name": "陈异为保护苗靖"},
            pick_first_value=lambda payload, paths: _pick_first_value(payload, paths),
            safe_float=lambda value: float(value) if value not in (None, "") else None,
            build_item_urls=lambda **kwargs: ("", ""),
            format_hms=lambda seconds: f"{seconds // 60:02d}:{seconds % 60:02d}",
            shorten_caption=lambda text: text,
            shorten_overview=lambda text: text,
        )

        result = factory.build(
            {"SeriesName": "野狗骨头", "Overview": "系列简介", "Item": {"Id": "episode-18", "Type": "Episode"}},
            action="stop",
            event_name="playback.stop",
            emby_config={},
            bot_config={"showIp": False, "showIpGeo": False, "showOverview": True},
        )

        self.assertEqual(result["templatePayload"]["overview_line"], "📖 剧情简介：暂无简介")

    def test_movie_uses_current_movie_overview(self):
        factory = PlaybackNotificationEventFactory(
            fetch_session_detail=lambda **kwargs: {},
            extract_item_id=lambda payload: "movie-1",
            fetch_item_detail=lambda **kwargs: {"Type": "Movie", "Name": "大军阀"},
            pick_first_value=lambda payload, paths: _pick_first_value(payload, paths),
            safe_float=lambda value: float(value) if value not in (None, "") else None,
            build_item_urls=lambda **kwargs: ("", ""),
            format_hms=lambda seconds: f"{seconds // 60:02d}:{seconds % 60:02d}",
            shorten_caption=lambda text: text,
            shorten_overview=lambda text: text,
        )

        result = factory.build(
            {"Item": {"Id": "movie-1", "Type": "Movie", "Overview": "北洋时代军阀们鱼肉百姓的故事。"}},
            action="stop",
            event_name="playback.stop",
            emby_config={},
            bot_config={"showIp": False, "showIpGeo": False, "showOverview": True},
        )

        self.assertEqual(result["templatePayload"]["overview_line"], "📖 剧情简介：北洋时代军阀们鱼肉百姓的故事。")

    def test_episode_headline_uses_human_friendly_title_format(self):
        factory = PlaybackNotificationEventFactory(
            fetch_session_detail=lambda **kwargs: {},
            extract_item_id=lambda payload: "episode-134",
            fetch_item_detail=lambda **kwargs: {
                "Type": "Episode",
                "SeriesName": "遮天",
                "Name": "灭杀圣联盟",
                "ParentIndexNumber": 1,
                "IndexNumber": 134,
                "RunTimeTicks": 25 * 60 * 10_000_000,
            },
            pick_first_value=lambda payload, paths: _pick_first_value(payload, paths),
            safe_float=lambda value: float(value) if value not in (None, "") else None,
            build_item_urls=lambda **kwargs: ("", ""),
            format_hms=lambda seconds: f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}" if seconds >= 3600 else f"{seconds // 60:02d}:{seconds % 60:02d}",
            shorten_caption=lambda text: text,
            shorten_overview=lambda text: text,
        )

        result = factory.build(
            {
                "UserName": "lishiya",
                "SeriesName": "遮天",
                "ItemName": "灭杀圣联盟",
                "ParentIndexNumber": 1,
                "IndexNumber": 134,
                "PositionTicks": 0,
                "RunTimeTicks": 25 * 60 * 10_000_000,
                "Client": "SenPlayer",
                "DeviceName": "iPhone11",
            },
            action="start",
            event_name="playback.start",
            emby_config={},
            bot_config={"showIp": False, "showIpGeo": False, "showOverview": False},
        )

        self.assertEqual(
            result["templatePayload"]["headline"],
            "🟢 【正在播放】遮天 - S1, Ep134 - 灭杀圣联盟",
        )
        self.assertEqual(
            result["templatePayload"]["title_line"],
            "🟢 【正在播放】遮天 - S1, Ep134 - 灭杀圣联盟",
        )

    def test_prefers_playback_info_position_ticks_for_progress_line(self):
        factory = PlaybackNotificationEventFactory(
            fetch_session_detail=lambda **kwargs: {},
            extract_item_id=lambda payload: "episode-134",
            fetch_item_detail=lambda **kwargs: {
                "Type": "Episode",
                "SeriesName": "遮天",
                "Name": "灭杀圣联盟",
                "ParentIndexNumber": 1,
                "IndexNumber": 134,
                "RunTimeTicks": 25 * 60 * 10_000_000,
            },
            pick_first_value=lambda payload, paths: _pick_first_value(payload, paths),
            safe_float=lambda value: float(value) if value not in (None, "") else None,
            build_item_urls=lambda **kwargs: ("", ""),
            format_hms=lambda seconds: f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}" if seconds >= 3600 else f"{seconds // 60:02d}:{seconds % 60:02d}",
            shorten_caption=lambda text: text,
            shorten_overview=lambda text: text,
        )

        result = factory.build(
            {
                "UserName": "lishiya",
                "SeriesName": "遮天",
                "ItemName": "灭杀圣联盟",
                "ParentIndexNumber": 1,
                "IndexNumber": 134,
                "PositionTicks": 0,
                "PlaybackInfo": {
                    "PositionTicks": 12 * 60 * 10_000_000 + 25 * 10_000_000,
                    "MediaSource": {"RunTimeTicks": 20 * 60 * 10_000_000 + 59 * 10_000_000},
                },
                "Client": "SenPlayer",
                "DeviceName": "iPhone11",
            },
            action="start",
            event_name="playback.start",
            emby_config={},
            bot_config={"showIp": False, "showIpGeo": False, "showOverview": False},
        )

        self.assertEqual(result["templatePayload"]["progress_line"], "▸ 进度：🟢 12:25 / 20:59 (59%)")

    def test_build_prefers_series_candidate_when_session_now_playing_exposes_episode_context(self):
        factory = PlaybackNotificationEventFactory(
            fetch_session_detail=lambda **kwargs: {
                "NowPlayingItem": {
                    "Id": "episode-1",
                    "Type": "Video",
                    "SeriesId": "series-1",
                    "ParentId": "season-1",
                    "SeriesName": "仙逆",
                    "ImageTags": {"Primary": "episode-tag"},
                }
            },
            extract_item_id=lambda payload: "episode-1",
            fetch_item_detail=lambda **kwargs: (
                {
                    "Id": "series-1",
                    "Type": "Series",
                    "Name": "仙逆",
                    "ImageTags": {"Primary": "series-tag"},
                }
                if kwargs.get("item_id") == "series-1"
                else (
                    {
                        "Id": "season-1",
                        "Type": "Season",
                        "Name": "Season 1",
                        "ImageTags": {"Primary": "season-tag"},
                    }
                    if kwargs.get("item_id") == "season-1"
                    else {}
                )
            ),
            pick_first_value=lambda payload, paths: _pick_first_value(payload, paths),
            safe_float=lambda value: float(value) if value not in (None, "") else None,
            build_item_urls=lambda **kwargs: ("", ""),
            format_hms=lambda seconds: f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}" if seconds >= 3600 else f"{seconds // 60:02d}:{seconds % 60:02d}",
            shorten_caption=lambda text: text,
            shorten_overview=lambda text: text,
        )

        result = factory.build(
            {
                "UserName": "lishiya",
                "ItemId": "episode-1",
                "SeriesName": "仙逆",
                "ParentIndexNumber": 1,
                "IndexNumber": 147,
                "DeviceName": "iPhone",
            },
            action="start",
            event_name="playback.start",
            emby_config={},
            bot_config={"showIp": False, "showIpGeo": False, "showOverview": False},
            session_id="session-1",
        )

        self.assertEqual(
            result["imageCandidates"][:3],
            [
                {"itemId": "series-1", "tag": "series-tag", "source": "series_primary"},
                {"itemId": "season-1", "tag": "season-tag", "source": "parent_primary"},
                {"itemId": "episode-1", "tag": "episode-tag", "source": "item_primary"},
            ],
        )

    def test_episode_image_candidates_follow_series_then_parent_then_item(self):
        candidates = build_playback_image_candidates(
            payload={},
            item_detail={
                "Type": "Episode",
                "Id": "episode-1",
                "SeriesId": "series-1",
                "SeriesPrimaryImageTag": "series-tag",
                "ParentId": "season-1",
                "ParentPrimaryImageTag": "season-tag",
                "ImageTags": {"Primary": "episode-tag"},
                "PrimaryImageItemId": "fallback-1",
            },
            item_id="episode-1",
            pick_first_value=lambda payload, paths: _pick_first_value(payload, paths),
        )

        self.assertEqual(
            candidates,
            [
                {"itemId": "series-1", "tag": "series-tag", "source": "series_primary"},
                {"itemId": "season-1", "tag": "season-tag", "source": "parent_primary"},
                {"itemId": "episode-1", "tag": "episode-tag", "source": "item_primary"},
                {"itemId": "fallback-1", "tag": "", "source": "primary_image_item"},
            ],
        )

    def test_movie_image_candidates_follow_item_then_series_then_parent(self):
        candidates = build_playback_image_candidates(
            payload={},
            item_detail={
                "Type": "Movie",
                "Id": "movie-1",
                "SeriesId": "series-1",
                "SeriesPrimaryImageTag": "series-tag",
                "ParentId": "parent-1",
                "ParentPrimaryImageTag": "parent-tag",
                "ImageTags": {"Primary": "movie-tag"},
                "PrimaryImageItemId": "fallback-1",
            },
            item_id="movie-1",
            pick_first_value=lambda payload, paths: _pick_first_value(payload, paths),
        )

        self.assertEqual(
            candidates,
            [
                {"itemId": "movie-1", "tag": "movie-tag", "source": "item_primary"},
                {"itemId": "series-1", "tag": "series-tag", "source": "series_primary"},
                {"itemId": "parent-1", "tag": "parent-tag", "source": "parent_primary"},
                {"itemId": "fallback-1", "tag": "", "source": "primary_image_item"},
            ],
        )

    def test_episode_candidates_prefer_series_detail_primary_when_episode_fields_missing(self):
        candidates = build_playback_image_candidates(
            payload={},
            item_detail={
                "Type": "Episode",
                "Id": "episode-1",
                "SeriesId": "series-1",
                "ParentId": "season-1",
                "ImageTags": {"Primary": "episode-tag"},
            },
            series_detail={
                "Id": "series-1",
                "Type": "Series",
                "ImageTags": {"Primary": "series-detail-tag"},
            },
            parent_detail={
                "Id": "season-1",
                "Type": "Season",
                "ImageTags": {"Primary": "season-detail-tag"},
            },
            item_id="episode-1",
            pick_first_value=lambda payload, paths: _pick_first_value(payload, paths),
        )

        self.assertEqual(
            candidates[:3],
            [
                {"itemId": "series-1", "tag": "series-detail-tag", "source": "series_primary"},
                {"itemId": "season-1", "tag": "season-detail-tag", "source": "parent_primary"},
                {"itemId": "episode-1", "tag": "episode-tag", "source": "item_primary"},
            ],
        )


class PlaybackNotificationImageResolverTests(unittest.TestCase):
    def test_prefers_same_candidate_for_photo_bytes_and_fallback_url(self):
        handler = dev_server.AppHandler.__new__(dev_server.AppHandler)
        logs = []
        handler._log_event = lambda **kwargs: logs.append(kwargs)
        handler._build_emby_primary_image_url_for_config = (
            lambda *, emby_config, item_id, image_tag="": f"https://img/{item_id}?tag={image_tag}"
        )

        def fake_fetch(*, emby_config, item_id, image_tag=""):
            if item_id == "series-1":
                raise RuntimeError("series fetch failed")
            if item_id == "season-1":
                return b"jpeg"
            return b""

        handler._fetch_emby_primary_image_for_config = fake_fetch

        result = handler._resolve_playback_notification_image_assets(
            emby_config={},
            item_id="episode-1",
            event_key="playback.stop",
            image_candidates=[
                {"itemId": "series-1", "tag": "series-tag", "source": "series_primary"},
                {"itemId": "season-1", "tag": "season-tag", "source": "parent_primary"},
                {"itemId": "episode-1", "tag": "episode-tag", "source": "item_primary"},
            ],
        )

        self.assertEqual(result["posterUrl"], "https://img/season-1?tag=")
        self.assertEqual(result["selectedCandidate"]["itemId"], "season-1")
        self.assertEqual(result["selectedCandidate"]["tag"], "season-tag")
        self.assertEqual(result["selectedCandidate"]["source"], "parent_primary")
        self.assertEqual(result["mode"], "photo_bytes")
        self.assertEqual(result["candidateCount"], 3)
        self.assertEqual(result["photoPayload"]["filename"], "season-1.jpg")
        self.assertTrue(logs)


def _pick_first_value(payload, paths):
    for path in paths:
        current = payload
        found = True
        for key in path:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                found = False
                break
        if found and current not in (None, ""):
            return current
    return ""


if __name__ == "__main__":
    unittest.main()
