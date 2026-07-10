import unittest
from types import SimpleNamespace
from unittest.mock import patch

import dev_server


class WebhookStatusTests(unittest.TestCase):
    def setUp(self):
        self._original_state = dict(dev_server.LAST_WEBHOOK_STATE)
        with dev_server.LAST_WEBHOOK_LOCK:
            dev_server.LAST_WEBHOOK_STATE.clear()
            dev_server.LAST_WEBHOOK_STATE.update(
                {
                    "lastReceivedAt": "",
                    "lastProcessed": None,
                    "lastPlaybackReceivedAt": "",
                    "lastPlaybackProcessed": None,
                }
            )

    def tearDown(self):
        with dev_server.LAST_WEBHOOK_LOCK:
            dev_server.LAST_WEBHOOK_STATE.clear()
            dev_server.LAST_WEBHOOK_STATE.update(self._original_state)

    def test_webhook_status_defaults_to_not_received_for_playback(self):
        payload = dev_server._build_webhook_status_payload()

        self.assertEqual(payload["playbackStatus"]["result"], "not_received")
        self.assertFalse(payload["playbackStatus"]["received"])
        self.assertIn("最近未收到 Emby 播放回调", payload["playbackStatus"]["detail"])

    def test_webhook_status_tracks_last_playback_processing(self):
        dev_server._mark_webhook_received("playback")
        dev_server._set_last_webhook_processed(
            event_type="playback",
            result="playback_event_disabled",
            detail="start 事件已关闭",
        )

        payload = dev_server._build_webhook_status_payload()

        self.assertTrue(payload["playbackStatus"]["received"])
        self.assertEqual(payload["playbackStatus"]["result"], "playback_event_disabled")
        self.assertEqual(payload["playbackStatus"]["lastProcessed"]["detail"], "start 事件已关闭")

    def test_guess_public_origins_from_store_prefers_public_callback_and_emby(self):
        original_reader = dev_server._read_store_unlocked
        try:
            dev_server._read_store_unlocked = lambda: {
                "notificationConfig": {
                    "channels": {
                        "wecom": {
                            "callbackUrl": "https://emby2.lshiya.top:333/api/bot/wecom_webhook",
                        }
                    }
                },
                "embyConfig": {
                    "serverUrl": "https://emby2.lshiya.top:333/emby",
                },
            }
            candidates = dev_server._guess_public_origins_from_store()
        finally:
            dev_server._read_store_unlocked = original_reader

        self.assertEqual(
            candidates,
            ["https://emby2.lshiya.top:333"],
        )

    def test_resolve_public_origin_prefers_vistamirror_public_base_url(self):
        handler = dev_server.AppHandler.__new__(dev_server.AppHandler)
        handler.headers = {"Host": "emby2.lshiya.top:333"}
        handler.server = SimpleNamespace(server_name="0.0.0.0", server_port=8091)

        with patch.dict(
            dev_server.os.environ,
            {
                "VISTAMIRROR_PUBLIC_BASE_URL": "https://vistamirror.lshiya.top:333",
                "BOT_PUBLIC_BASE_URL": "https://legacy.example.com:333",
            },
            clear=False,
        ):
            origin = handler._resolve_public_origin()

        self.assertEqual(origin, "https://vistamirror.lshiya.top:333")

    def test_handle_bot_webhook_url_get_reports_env_source(self):
        handler = dev_server.AppHandler.__new__(dev_server.AppHandler)
        handler.headers = {"Host": "emby2.lshiya.top:333"}
        handler.server = SimpleNamespace(server_name="0.0.0.0", server_port=8091)
        responses = []
        handler._send_json = lambda status, payload, **kwargs: responses.append((status, payload, kwargs))

        with patch.dict(
            dev_server.os.environ,
            {
                "VISTAMIRROR_PUBLIC_BASE_URL": "https://vistamirror.lshiya.top:333",
                "BOT_WEBHOOK_TOKEN": "vistamirror",
            },
            clear=False,
        ):
            handler._handle_bot_webhook_url_get()

        self.assertEqual(responses[0][0], 200)
        self.assertEqual(
            responses[0][1]["webhookUrl"],
            "https://vistamirror.lshiya.top:333/api/v1/webhook?token=vistamirror",
        )
        self.assertEqual(responses[0][1]["source"], "env:VISTAMIRROR_PUBLIC_BASE_URL")
        self.assertEqual(responses[0][1]["preferredEnv"], "VISTAMIRROR_PUBLIC_BASE_URL")

    def test_coerce_form_payload_prefers_embedded_json_payload(self):
        handler = dev_server.AppHandler.__new__(dev_server.AppHandler)
        payload = handler._coerce_form_payload(
            {
                "data": ['{"Event":"PlaybackStart","ItemId":"123","UserName":"demo"}'],
            }
        )

        self.assertEqual(payload["Event"], "PlaybackStart")
        self.assertEqual(payload["ItemId"], "123")

    def test_coerce_form_payload_keeps_flat_form_fields(self):
        handler = dev_server.AppHandler.__new__(dev_server.AppHandler)
        payload = handler._coerce_form_payload(
            {
                "Event": ["PlaybackPause"],
                "ItemId": ["456"],
                "UserName": ["demo"],
            }
        )

        self.assertEqual(
            payload,
            {"Event": "PlaybackPause", "ItemId": "456", "UserName": "demo"},
        )

    def test_playback_user_scope_matches_selected_username(self):
        matched, detail = dev_server._playback_user_scope_matches(
            {
                "runtime": {
                    "playback": {
                        "userScope": {
                            "mode": "selected",
                            "selectedUserNames": ["lishiya"],
                            "selectedUsersMeta": [{"id": "u1", "name": "lishiya"}],
                        }
                    }
                }
            },
            user_name="lishiya",
            user_id="u1",
        )

        self.assertTrue(matched)
        self.assertEqual(detail, "")

    def test_playback_user_scope_rejects_unselected_username(self):
        matched, detail = dev_server._playback_user_scope_matches(
            {
                "runtime": {
                    "playback": {
                        "userScope": {
                            "mode": "selected",
                            "selectedUserNames": ["lishiya"],
                            "selectedUsersMeta": [{"id": "u1", "name": "lishiya"}],
                        }
                    }
                }
            },
            user_name="admin",
            user_id="u2",
        )

        self.assertFalse(matched)
        self.assertIn("admin", detail)

    def test_normalize_playback_user_rows_handles_query_payload(self):
        users = dev_server._normalize_playback_user_rows(
            {
                "Items": [
                    {"Id": "u2", "Name": "admin"},
                    {"Id": "u1", "Name": "lishiya"},
                ]
            }
        )

        self.assertEqual(
            users,
            [
                {"id": "u2", "name": "admin", "disabled": False},
                {"id": "u1", "name": "lishiya", "disabled": False},
            ],
        )

    def test_classify_playback_ignores_media_type_and_name_as_event_marker(self):
        handler = dev_server.AppHandler.__new__(dev_server.AppHandler)

        event_type, event_name = handler._classify_webhook_type(
            {
                "Event": "PlaybackStart",
                "NotificationType": "PlaybackStart",
                "Type": "Video",
                "Name": "某一集",
            }
        )

        self.assertEqual(event_type, "playback")
        self.assertEqual(event_name, "PlaybackStart")
        self.assertEqual(
            handler._detect_playback_action(
                {
                    "Event": "PlaybackStart",
                    "NotificationType": "PlaybackStart",
                    "Type": "Video",
                    "Name": "某一集",
                },
                event_name,
            ),
            "start",
        )


if __name__ == "__main__":
    unittest.main()
