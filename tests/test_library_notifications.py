import json
import pathlib
import tempfile
import time
import unittest

from backend_modules.telegram_commands import TelegramCommandService


class FakeSender:
    def __init__(self):
        self.photos = []
        self.texts = []
        self.fail_photo = False

    def send_photo_file(self, **kwargs):
        if self.fail_photo:
            raise RuntimeError("photo failed")
        self.photos.append(kwargs)

    def send_text(self, **kwargs):
        self.texts.append(kwargs)


class LibraryNotificationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.temp_dir.name)
        self.store_path = root / "invites.json"
        self.store_path.write_text(
            json.dumps(
                {
                    "botConfig": {
                        "enableCore": True,
                        "enableLibrary": True,
                        "telegramToken": "123:test",
                        "telegramChatId": "100",
                    },
                    "embyConfig": {
                        "serverUrl": "http://emby.local",
                        "apiKey": "secret",
                    },
                }
            ),
            encoding="utf-8",
        )
        self.service = TelegramCommandService(
            store_path=self.store_path,
            event_log_path=root / "events.jsonl",
        )
        self.sender = FakeSender()
        self.service.sender = self.sender
        self.rows = [
            {
                "Id": "movie-old",
                "Type": "Movie",
                "Name": "已有电影",
                "DateCreated": "2026-06-20T10:00:00Z",
            }
        ]

        def fake_emby_get(path):
            if path.startswith("/Items?") and "Ids=episode-new" in path:
                return {
                    "Items": [
                        {
                            "Id": "episode-new",
                            "Type": "Episode",
                            "Name": "特别篇：逐冥之役",
                            "SeriesName": "云深不知梦",
                            "SeriesId": "series-1",
                            "ParentIndexNumber": 1,
                            "IndexNumber": 1,
                            "DateCreated": "2026-06-20T12:59:00Z",
                            "Overview": "单集简介：许安在逐冥战场第一次正面交锋。",
                        }
                    ]
                }
            if path.startswith("/Items?") and "Ids=episode-no-overview" in path:
                return {
                    "Items": [
                        {
                            "Id": "episode-no-overview",
                            "Type": "Episode",
                            "Name": "第 2 集",
                            "SeriesName": "云深不知梦",
                            "SeriesId": "series-1",
                            "ParentIndexNumber": 1,
                            "IndexNumber": 2,
                            "DateCreated": "2026-06-20T13:01:00Z",
                        }
                    ]
                }
            if path.startswith("/Items?") and "Ids=episode-batch-1" in path:
                return {
                    "Items": [
                        {
                            "Id": "episode-batch-1",
                            "Type": "Episode",
                            "Name": "第 1 集",
                            "SeriesName": "南部档案",
                            "SeriesId": "series-batch",
                            "ParentIndexNumber": 1,
                            "IndexNumber": 1,
                            "DateCreated": "2026-06-23T15:10:00Z",
                        }
                    ]
                }
            if path.startswith("/Items?") and "Ids=episode-batch-2" in path:
                return {
                    "Items": [
                        {
                            "Id": "episode-batch-2",
                            "Type": "Episode",
                            "Name": "第 2 集",
                            "SeriesName": "南部档案",
                            "SeriesId": "series-batch",
                            "ParentIndexNumber": 1,
                            "IndexNumber": 2,
                            "DateCreated": "2026-06-23T15:10:00Z",
                        }
                    ]
                }
            if path.startswith("/Items?") and "Ids=episode-batch-3" in path:
                return {
                    "Items": [
                        {
                            "Id": "episode-batch-3",
                            "Type": "Episode",
                            "Name": "第 3 集",
                            "SeriesName": "南部档案",
                            "SeriesId": "series-batch",
                            "ParentIndexNumber": 1,
                            "IndexNumber": 3,
                            "DateCreated": "2026-06-23T15:10:00Z",
                        }
                    ]
                }
            if path.startswith("/Items?") and "Ids=series-1" in path:
                return {
                    "Items": [
                        {
                            "Id": "series-1",
                            "Type": "Series",
                            "Name": "云深不知梦",
                            "ProductionYear": 2026,
                            "CommunityRating": 7,
                            "Overview": "上古末世，辉华族少女自惨痛的族地叛乱中出逃。",
                            "Genres": ["动画", "国漫"],
                            "People": [{"Name": "蔡海婷"}, {"Name": "张若瑜"}],
                        }
                    ]
                }
            if path.startswith("/Items?") and "Ids=series-batch" in path:
                return {
                    "Items": [
                        {
                            "Id": "series-batch",
                            "Type": "Series",
                            "Name": "南部档案",
                            "ProductionYear": 2026,
                            "CommunityRating": 7,
                            "Overview": "民国初年，南洋上发生水鬼望乡离奇命案。",
                            "Genres": ["华语剧集"],
                            "People": [{"Name": "张新成"}, {"Name": "丁禹兮"}],
                        }
                    ]
                }
            if path.startswith("/Items?") and "Ids=movie-new" in path:
                return {
                    "Items": [
                        {
                            "Id": "movie-new",
                            "Type": "Movie",
                            "Name": "新电影",
                            "ProductionYear": 2026,
                            "DateCreated": "2026-06-20T13:00:00Z",
                            "Overview": "电影简介",
                            "Genres": ["剧情"],
                            "People": [{"Name": "主演甲"}],
                            "CommunityRating": 8.5,
                        }
                    ]
                }
            if path.startswith("/Items?"):
                return {"Items": list(self.rows)}
            raise AssertionError(f"unexpected Emby path: {path}")

        self.service._emby_get = fake_emby_get
        self.service._fetch_emby_primary_image = lambda item_id: b"jpeg"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_first_poll_creates_baseline_without_notifications(self):
        self.service._poll_library_notifications_once()

        self.assertFalse(self.sender.photos)
        self.assertFalse(self.sender.texts)
        state = self.service._read_library_notification_state()
        self.assertTrue(state["active"])
        self.assertIn("movie-old", state["seen"])

    def test_second_poll_sends_new_episode_card_and_deduplicates_webhook(self):
        self.service._poll_library_notifications_once()
        self.rows.insert(
            0,
            {
                "Id": "episode-new",
                "Type": "Episode",
                "Name": "特别篇：逐冥之役",
                "SeriesName": "云深不知梦",
                "SeriesId": "series-1",
                "ParentIndexNumber": 1,
                "IndexNumber": 1,
                "DateCreated": "2026-06-20T12:59:00Z",
            },
        )

        self.service._poll_library_notifications_once()

        self.assertEqual(len(self.sender.photos), 1)
        caption = self.sender.photos[0]["caption"]
        self.assertIn("🎬 【单集入库】｜ 云深不知梦（2026）", caption)
        self.assertIn("✨ 评分：7 / 10 ｜ 🔄 状态：整理完成", caption)
        self.assertIn("💬 “单集简介：许安在逐冥战场第一次正面交锋”", caption)
        self.assertIn("🏷️ 内容类型 ｜ 电视剧 · 动画 / 国漫", caption)
        self.assertIn("▶️ 当前更新 ｜ S01E01", caption)
        self.assertIn("💿 资源规格 ｜ 质量未知 (1 个文件)", caption)
        self.assertIn("🤖 主演阵容 ｜ 蔡海婷、张若瑜", caption)
        self.assertIn("= 📖内容简介 =", caption)
        self.assertIn("单集简介：许安在逐冥战场第一次正面交锋。", caption)

        duplicate = self.service.notify_library_item(
            item_id="episode-new",
            payload=self.rows[0],
            source="webhook",
        )
        self.assertEqual(duplicate["status"], "duplicate")
        self.assertEqual(len(self.sender.photos), 1)

    def test_single_episode_without_episode_overview_falls_back_to_series_overview(self):
        self.service._poll_library_notifications_once()
        self.rows.insert(
            0,
            {
                "Id": "episode-no-overview",
                "Type": "Episode",
                "Name": "第 2 集",
                "SeriesName": "云深不知梦",
                "SeriesId": "series-1",
                "ParentIndexNumber": 1,
                "IndexNumber": 2,
                "DateCreated": "2026-06-20T13:01:00Z",
            },
        )

        self.service._poll_library_notifications_once()

        self.assertEqual(len(self.sender.photos), 1)
        caption = self.sender.photos[0]["caption"]
        self.assertIn("💬 “上古末世，辉华族少女自惨痛的族地叛乱中出逃”", caption)
        self.assertIn("上古末世，辉华族少女自惨痛的族地叛乱中出逃。", caption)

    def test_poll_groups_multiple_new_episodes_into_one_series_card(self):
        self.service._poll_library_notifications_once()
        self.rows = [
            {
                "Id": "episode-batch-1",
                "Type": "Episode",
                "Name": "第 1 集",
                "SeriesName": "南部档案",
                "SeriesId": "series-batch",
                "ParentIndexNumber": 1,
                "IndexNumber": 1,
                "DateCreated": "2026-06-23T15:10:00Z",
            },
            {
                "Id": "episode-batch-2",
                "Type": "Episode",
                "Name": "第 2 集",
                "SeriesName": "南部档案",
                "SeriesId": "series-batch",
                "ParentIndexNumber": 1,
                "IndexNumber": 2,
                "DateCreated": "2026-06-23T15:10:01Z",
            },
            {
                "Id": "episode-batch-3",
                "Type": "Episode",
                "Name": "第 3 集",
                "SeriesName": "南部档案",
                "SeriesId": "series-batch",
                "ParentIndexNumber": 1,
                "IndexNumber": 3,
                "DateCreated": "2026-06-23T15:10:02Z",
            },
        ]

        self.service._poll_library_notifications_once()

        self.assertEqual(len(self.sender.photos), 1)
        caption = self.sender.photos[0]["caption"]
        self.assertIn("📺 【整季入库】｜ 南部档案（2026）", caption)
        self.assertIn("📑 收录进度 ｜ S01 E01-E03", caption)
        self.assertIn("💿 资源规格 ｜ 质量未知 (3 个文件)", caption)
        self.assertIn("🏷️ 内容类型 ｜ 电视剧 · 华语剧集", caption)
        self.assertIn("民国初年，南洋上发生水鬼望乡离奇命案。", caption)
        state = self.service._read_library_notification_state()
        self.assertIn("episode-batch-1", state["seen"])
        self.assertIn("episode-batch-2", state["seen"])
        self.assertIn("episode-batch-3", state["seen"])
        self.assertFalse(state["pendingSeries"])

    def test_webhook_episode_is_buffered_then_flushed_as_group(self):
        result1 = self.service.notify_library_item(
            item_id="episode-batch-1",
            payload={
                "Id": "episode-batch-1",
                "Type": "Episode",
                "Name": "第 1 集",
                "SeriesName": "南部档案",
                "SeriesId": "series-batch",
                "ParentIndexNumber": 1,
                "IndexNumber": 1,
                "DateCreated": "2026-06-23T15:10:00Z",
            },
            source="webhook",
        )
        result2 = self.service.notify_library_item(
            item_id="episode-batch-2",
            payload={
                "Id": "episode-batch-2",
                "Type": "Episode",
                "Name": "第 2 集",
                "SeriesName": "南部档案",
                "SeriesId": "series-batch",
                "ParentIndexNumber": 1,
                "IndexNumber": 2,
                "DateCreated": "2026-06-23T15:10:01Z",
            },
            source="webhook",
        )

        self.assertEqual(result1["status"], "buffered")
        self.assertEqual(result2["status"], "buffered")
        self.assertFalse(self.sender.photos)

        state = self.service._read_library_notification_state()
        group = state["pendingSeries"]["series-batch"]
        group["lastSeenAt"] = "2026-06-23T15:09:00"
        state["pendingSeries"]["series-batch"] = group
        self.service._write_library_notification_state(state)

        self.service._flush_library_notification_groups_due()

        self.assertEqual(len(self.sender.photos), 1)
        caption = self.sender.photos[0]["caption"]
        self.assertIn("📑 收录进度 ｜ S01 E01-E02", caption)
        state = self.service._read_library_notification_state()
        self.assertIn("episode-batch-1", state["seen"])
        self.assertIn("episode-batch-2", state["seen"])
        self.assertFalse(state["pendingSeries"])

    def test_photo_failure_falls_back_to_text_and_marks_sent(self):
        self.service._poll_library_notifications_once()
        self.sender.fail_photo = True

        result = self.service.notify_library_item(
            item_id="movie-new",
            payload={"Id": "movie-new", "Type": "Movie", "Name": "新电影"},
            source="webhook",
        )

        self.assertEqual(result["status"], "sent")
        self.assertFalse(result["photo"])
        self.assertEqual(len(self.sender.texts), 1)
        self.assertIn("🎬 【电影入库】｜ 新电影（2026）", self.sender.texts[0]["text"])
        self.assertIn("💬 “电影简介”", self.sender.texts[0]["text"])
        self.assertIn("🏷️ 内容类型 ｜ 电影 · 剧情", self.sender.texts[0]["text"])
        self.assertIn("movie-new", self.service._read_library_notification_state()["seen"])

    def test_custom_single_library_template_is_used(self):
        store = json.loads(self.store_path.read_text(encoding="utf-8"))
        store["botConfig"]["libraryTemplates"] = {
            "single": "通知：{{title}}{{year_suffix}} | {{episode_info}} | {{file_count}}",
            "grouped": "{{title}} / {{episode_info}} / {{file_count}}",
        }
        self.store_path.write_text(json.dumps(store), encoding="utf-8")

        result = self.service.notify_library_item(
            item_id="movie-new",
            payload={"Id": "movie-new", "Type": "Movie", "Name": "新电影"},
            source="webhook",
        )

        self.assertEqual(result["status"], "sent")
        self.assertEqual(len(self.sender.photos), 1)
        self.assertEqual(self.sender.photos[0]["caption"], "通知：新电影（2026） | 电影 | 1")

    def test_custom_grouped_library_template_is_used(self):
        store = json.loads(self.store_path.read_text(encoding="utf-8"))
        store["botConfig"]["libraryTemplates"] = {
            "single": "{{title}}",
            "grouped": "合并通知：{{title}}{{year_suffix}} / {{episode_info}} / {{file_count}}",
        }
        self.store_path.write_text(json.dumps(store), encoding="utf-8")
        self.service._poll_library_notifications_once()
        self.rows = [
            {
                "Id": "episode-batch-1",
                "Type": "Episode",
                "Name": "第 1 集",
                "SeriesName": "南部档案",
                "SeriesId": "series-batch",
                "ParentIndexNumber": 1,
                "IndexNumber": 1,
                "DateCreated": "2026-06-23T15:10:00Z",
            },
            {
                "Id": "episode-batch-2",
                "Type": "Episode",
                "Name": "第 2 集",
                "SeriesName": "南部档案",
                "SeriesId": "series-batch",
                "ParentIndexNumber": 1,
                "IndexNumber": 2,
                "DateCreated": "2026-06-23T15:10:01Z",
            },
        ]

        self.service._poll_library_notifications_once()

        self.assertEqual(len(self.sender.photos), 1)
        self.assertEqual(self.sender.photos[0]["caption"], "合并通知：南部档案（2026） / S01 E01-E02 / 2")

    def test_library_notification_tagline_falls_back_to_first_overview_sentence(self):
        text = self.service._library_notification_tagline_text(
            {"Overview": "第一句。第二句。", "Tagline": "", "Taglines": []}
        )
        self.assertEqual(text, "第一句")

    def test_library_notification_status_maps_to_chinese(self):
        self.assertEqual(self.service._library_notification_status_text({"Status": "Continuing"}), "连载中")
        self.assertEqual(self.service._library_notification_status_text({"Status": ""}), "整理完成")

    def test_disabled_notification_does_not_send(self):
        store = json.loads(self.store_path.read_text(encoding="utf-8"))
        store["botConfig"]["enableLibrary"] = False
        self.store_path.write_text(json.dumps(store), encoding="utf-8")

        result = self.service.notify_library_item(
            item_id="movie-new",
            payload={"Id": "movie-new", "Type": "Movie", "Name": "新电影"},
            source="webhook",
        )

        self.assertEqual(result["status"], "disabled")
        self.assertFalse(self.sender.photos)
        self.assertFalse(self.sender.texts)

    def test_item_detail_uses_items_by_id_endpoint(self):
        requested = []

        def fake_get(path):
            requested.append(path)
            return {"Items": [{"Id": "episode-new", "Type": "Episode", "Name": "第1集"}]}

        self.service._emby_get = fake_get
        detail = self.service._fetch_library_item_detail("episode-new")

        self.assertEqual(detail["Id"], "episode-new")
        self.assertEqual(detail["_detailSource"], "items_by_id")
        self.assertIn("Ids=episode-new", requested[0])
        self.assertNotIn("/Items/episode-new", requested[0])

    def test_item_detail_falls_back_to_user_endpoint(self):
        requested = []

        def fake_get(path):
            requested.append(path)
            if path.startswith("/Items?"):
                return {"Items": []}
            if path.startswith("/Users/user-1/Items/episode-new"):
                return {"Id": "episode-new", "Type": "Episode", "Name": "第1集"}
            raise AssertionError(path)

        self.service._emby_get = fake_get
        self.service._resolve_emby_user_id = lambda: "user-1"
        detail = self.service._fetch_library_item_detail("episode-new")

        self.assertEqual(detail["_detailSource"], "user_item")
        self.assertTrue(any(path.startswith("/Users/user-1/Items/episode-new") for path in requested))

    def test_failed_item_does_not_block_later_item(self):
        self.service._poll_library_notifications_once()
        self.rows = [
            {"Id": "episode-new", "Type": "Episode", "Name": "特别篇：逐冥之役", "SeriesName": "云深不知梦", "SeriesId": "series-1", "ParentIndexNumber": 1, "IndexNumber": 1, "DateCreated": "2026-06-20T12:59:00Z"},
            {"Id": "movie-new", "Type": "Movie", "Name": "新电影", "DateCreated": "2026-06-20T13:00:00Z"},
        ]

        class SelectiveSender(FakeSender):
            def send_photo_file(self, **kwargs):
                if "云深不知梦" in kwargs.get("caption", ""):
                    raise RuntimeError("telegram unavailable")
                self.photos.append(kwargs)

            def send_text(self, **kwargs):
                if "云深不知梦" in kwargs.get("text", ""):
                    raise RuntimeError("telegram unavailable")
                self.texts.append(kwargs)

        self.sender = SelectiveSender()
        self.service.sender = self.sender
        self.service._stop_event.set()  # skip the inter-message wait in this unit test

        self.service._poll_library_notifications_once()

        state = self.service._read_library_notification_state()
        self.assertNotIn("episode-new", state["seen"])
        self.assertIn("movie-new", state["seen"])
        self.assertEqual(len(self.sender.photos), 1)

    def test_telegram_failure_is_not_marked_as_notified(self):
        class BrokenSender(FakeSender):
            def send_photo_file(self, **kwargs):
                raise RuntimeError("telegram unavailable")

            def send_text(self, **kwargs):
                raise RuntimeError("telegram unavailable")

        self.service.sender = BrokenSender()

        with self.assertRaises(RuntimeError):
            self.service.notify_library_item(
                item_id="movie-new",
                payload={"Id": "movie-new", "Type": "Movie", "Name": "新电影"},
                source="poll",
            )

        self.assertNotIn("movie-new", self.service._read_library_notification_state()["seen"])


if __name__ == "__main__":
    unittest.main()
