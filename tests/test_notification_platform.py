import unittest

from backend_modules.notification_platform import (
    NotificationDispatchService,
    build_notification_preview,
    migrate_bot_config_to_notification_config,
    notification_capabilities,
    normalize_notification_config,
    sync_notification_config_to_bot_config,
    validate_notification_config,
)


class FakeTelegramSender:
    def __init__(self):
        self.texts = []
        self.photos = []

    def send_text(self, **kwargs):
        self.texts.append(kwargs)

    def send_photo(self, **kwargs):
        self.photos.append(kwargs)

    def send_photo_file(self, **kwargs):
        self.photos.append(kwargs)


class FakeTelegramSenderWithFailures(FakeTelegramSender):
    def __init__(self, *, fail_photo=False, fail_photo_file=False):
        super().__init__()
        self.fail_photo = fail_photo
        self.fail_photo_file = fail_photo_file

    def send_photo(self, **kwargs):
        if self.fail_photo:
            raise RuntimeError("Telegram 请求失败（HTTP 400）：Bad Request: wrong type of the web page content")
        super().send_photo(**kwargs)

    def send_photo_file(self, **kwargs):
        if self.fail_photo_file:
            raise RuntimeError("Telegram 请求失败：photo upload failed")
        super().send_photo_file(**kwargs)


class NotificationPlatformTests(unittest.TestCase):
    def test_normalize_notification_config_upgrades_legacy_playback_template_only(self):
        config = normalize_notification_config(
            {
                "templates": {
                    "telegram": {
                        "playback.start": "{{headline}}\n\n{{meta_line}}\n{{progress_line}}\n{{ip_line}}\n{{device_line}}\n🕒 时间：{{occurred_at}}\n{{overview_block}}",
                        "library.single": "自定义 {{title}}",
                    }
                }
            }
        )

        self.assertEqual(
            config["templates"]["telegram"]["playback.start"],
            "{{title_line}}\n\n{{user_line}}\n{{playback_method_line}}\n{{media_spec_line}}\n━━━━━━━━━━━━━━━━━━━━\n📋 播放数据\n{{rating_line}}\n{{progress_line}}\n\n🛋️ 终端状态\n{{device_line}}\n{{ip_line}}\n{{time_line}}\n\n━━━━━━━━━━━━━━━━━━━━\n{{overview_line}}",
        )
        self.assertEqual(config["templates"]["telegram"]["library.single"], "自定义 {{title}}")

    def test_migrate_bot_config_keeps_library_templates_and_routes(self):
        migrated = migrate_bot_config_to_notification_config(
            {
                "enableCore": True,
                "enablePlayback": True,
                "enableLibrary": True,
                "telegramToken": "123:test",
                "telegramChatId": "100",
                "enableCommands": True,
                "notifyEvents": {"start": True, "pause": False, "resume": True, "stop": False},
                "libraryTemplates": {"single": "single {{title}}", "grouped": "grouped {{title}}"},
            }
        )

        self.assertEqual(migrated["channels"]["telegram"]["botToken"], "123:test")
        self.assertEqual(migrated["channels"]["telegram"]["chatId"], "100")
        self.assertTrue(migrated["routes"]["telegram"]["playback.start"])
        self.assertFalse(migrated["routes"]["telegram"]["playback.pause"])
        self.assertEqual(migrated["templates"]["telegram"]["library.single"], "single {{title}}")
        self.assertEqual(migrated["templates"]["telegram"]["library.grouped"], "grouped {{title}}")

    def test_normalize_notification_config_preserves_custom_display_copy(self):
        config = normalize_notification_config(
            {
                "display": {
                    "telegram": {
                        "playback.start": {
                            "label": "正在热播提醒",
                            "description": "用户一开始播放就推送这张卡片。",
                        }
                    }
                }
            }
        )

        self.assertEqual(config["display"]["telegram"]["playback.start"]["label"], "正在热播提醒")
        self.assertEqual(config["display"]["telegram"]["playback.start"]["description"], "用户一开始播放就推送这张卡片。")
        self.assertEqual(config["display"]["telegram"]["playback.pause"]["label"], "暂停播放")

    def test_normalize_notification_config_preserves_playback_user_scope(self):
        config = normalize_notification_config(
            {
                "runtime": {
                    "playback": {
                        "userScope": {
                            "mode": "selected",
                            "selectedUserNames": ["lishiya", "admin", "lishiya"],
                            "selectedUsersMeta": [
                                {"id": "u1", "name": "lishiya"},
                                {"id": "u2", "name": "admin"},
                            ],
                        }
                    }
                }
            }
        )

        self.assertEqual(config["runtime"]["playback"]["userScope"]["mode"], "selected")
        self.assertEqual(config["runtime"]["playback"]["userScope"]["selectedUserNames"], ["lishiya", "admin"])
        self.assertEqual(
            config["runtime"]["playback"]["userScope"]["selectedUsersMeta"],
            [{"id": "u1", "name": "lishiya"}, {"id": "u2", "name": "admin"}],
        )

    def test_validate_notification_config_requires_selected_playback_users(self):
        config, error = validate_notification_config(
            {
                "runtime": {
                    "playback": {
                        "userScope": {
                            "mode": "selected",
                            "selectedUserNames": [],
                            "selectedUsersMeta": [],
                        }
                    }
                }
            }
        )

        self.assertIsNone(config)
        self.assertIn("至少需要勾选 1 个 Emby 用户", error or "")

    def test_sync_notification_config_back_to_bot_config(self):
        notification = normalize_notification_config(
            {
                "enabled": True,
                "channels": {
                    "telegram": {
                        "enabled": True,
                        "botToken": "321:test",
                        "chatId": "200",
                        "enableCommands": False,
                    }
                },
                "routes": {
                    "telegram": {
                        "playback.start": True,
                        "playback.pause": False,
                        "playback.resume": False,
                        "playback.stop": True,
                        "library.single": True,
                        "library.grouped": False,
                    }
                },
            }
        )

        bot = sync_notification_config_to_bot_config(notification)

        self.assertEqual(bot["telegramToken"], "321:test")
        self.assertEqual(bot["telegramChatId"], "200")
        self.assertFalse(bot["enableCommands"])
        self.assertTrue(bot["notifyEvents"]["start"])
        self.assertFalse(bot["notifyEvents"]["pause"])
        self.assertTrue(bot["enableLibrary"])

    def test_preview_reports_missing_variables(self):
        preview = build_notification_preview(
            channel="telegram",
            event_key="library.single",
            template="标题：{{title}}\n未知：{{unknown_var}}",
            sample_key="singleMovie",
        )

        self.assertIn("标题：新电影", preview["previewText"])
        self.assertIn("unknown_var", preview["missingVariables"])

    def test_preview_uses_requested_sample_payload(self):
        preview = build_notification_preview(
            channel="telegram",
            event_key="library.single",
            template="剧集：{{title}} {{episode_info}}",
            sample_key="singleEpisode",
        )

        self.assertEqual(preview["sampleKey"], "singleEpisode")
        self.assertIn("云深不知梦", preview["previewText"])
        self.assertIn("S01E01", preview["previewText"])

    def test_preview_applies_payload_overrides(self):
        preview = build_notification_preview(
            channel="telegram",
            event_key="playback.start",
            template="{{title_line}}\n{{ip_line}}\n{{overview_line}}",
            sample_key="default",
            payload_overrides={
                "ip_line": "",
                "overview_line": "",
            },
        )

        self.assertEqual(preview["previewText"], "🟢 【正在播放】云深不知梦 - S1, Ep1 - 特别篇：逐冥之役")

    def test_notification_capabilities_exposes_real_events_and_templates(self):
        capabilities = notification_capabilities()
        events = capabilities["events"]
        event = next((row for row in events if row.get("key") == "library.grouped"), None)

        self.assertIsNotNone(event)
        self.assertEqual(event["supportedChannels"], ["telegram", "wecom"])
        self.assertTrue(event["defaultTemplateByChannel"]["telegram"])
        self.assertTrue(event["defaultTemplateByChannel"]["wecom"])
        self.assertIn("episode_info", [row["key"] for row in event["variables"]])
        self.assertTrue(any(str(row.get("description") or "").strip() for row in event["variables"]))

    def test_dispatcher_sends_telegram_photo_file(self):
        sender = FakeTelegramSender()
        dispatcher = NotificationDispatchService(telegram_sender=sender)
        config = normalize_notification_config(
            {
                "enabled": True,
                "channels": {
                    "telegram": {
                        "enabled": True,
                        "botToken": "123:test",
                        "chatId": "100",
                        "enableCommands": True,
                    }
                },
                "routes": {"telegram": {"library.single": True}},
                "templates": {"telegram": {"library.single": "🎬 {{title}}{{year_suffix}}"}},
            }
        )

        result = dispatcher.dispatch(
            config=config,
            event={
                "eventKey": "library.single",
                "payload": {"title": "新电影", "year_suffix": "（2026）"},
                "channelContext": {
                    "telegram": {
                        "photoBytes": b"jpeg",
                        "filename": "poster.jpg",
                        "contentType": "image/jpeg",
                    }
                },
            },
        )

        self.assertTrue(result["ok"])
        self.assertEqual(len(sender.photos), 1)
        self.assertEqual(sender.photos[0]["caption"], "🎬 新电影（2026）")
        self.assertFalse(sender.texts)

    def test_dispatcher_falls_back_to_text_with_button_when_photo_url_fails(self):
        sender = FakeTelegramSenderWithFailures(fail_photo=True)
        dispatcher = NotificationDispatchService(telegram_sender=sender)
        config = normalize_notification_config(
            {
                "enabled": True,
                "channels": {
                    "telegram": {
                        "enabled": True,
                        "botToken": "123:test",
                        "chatId": "100",
                    }
                },
                "routes": {"telegram": {"playback.start": True}},
                "templates": {"telegram": {"playback.start": "{{headline}}"}},
            }
        )

        result = dispatcher.dispatch(
            config=config,
            event={
                "eventKey": "playback.start",
                "payload": {"headline": "开始播放"},
                "channelContext": {
                    "telegram": {
                        "photoUrl": "https://example.com/poster.jpg",
                        "buttonText": "🔗 跳转详情",
                        "buttonUrl": "https://example.com/detail",
                    }
                },
            },
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["results"][0]["mode"], "text")
        self.assertEqual(len(sender.texts), 1)
        self.assertEqual(
            sender.texts[0]["reply_markup"],
            {"inline_keyboard": [[{"text": "🔗 跳转详情", "url": "https://example.com/detail"}]]},
        )

    def test_dispatcher_falls_back_to_text_with_button_when_photo_file_fails(self):
        sender = FakeTelegramSenderWithFailures(fail_photo_file=True)
        dispatcher = NotificationDispatchService(telegram_sender=sender)
        config = normalize_notification_config(
            {
                "enabled": True,
                "channels": {
                    "telegram": {
                        "enabled": True,
                        "botToken": "123:test",
                        "chatId": "100",
                    }
                },
                "routes": {"telegram": {"playback.pause": True}},
                "templates": {"telegram": {"playback.pause": "{{headline}}"}},
            }
        )

        result = dispatcher.dispatch(
            config=config,
            event={
                "eventKey": "playback.pause",
                "payload": {"headline": "暂停播放"},
                "channelContext": {
                    "telegram": {
                        "photoBytes": b"jpeg",
                        "buttonText": "🔗 跳转详情",
                        "buttonUrl": "https://example.com/detail",
                    }
                },
            },
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["results"][0]["mode"], "text_fallback")
        self.assertEqual(len(sender.texts), 1)
        self.assertEqual(
            sender.texts[0]["reply_markup"],
            {"inline_keyboard": [[{"text": "🔗 跳转详情", "url": "https://example.com/detail"}]]},
        )


if __name__ == "__main__":
    unittest.main()
