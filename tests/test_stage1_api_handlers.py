import json
import os
import pathlib
import tempfile
import threading
import unittest
from unittest.mock import patch

import dev_server
from backend_modules.api_handlers import cover_studio_handlers
from backend_modules.api_handlers import config_handlers, invite_sync_handlers
from backend_modules.store import app_store


class _DummyHandler:
    def __init__(self, payload=None):
        self.payload = payload
        self.responses = []
        self.logs = []

    def _read_json_body(self):
        return self.payload

    def _send_json(self, status, payload, **kwargs):
        self.responses.append((status, payload, kwargs))

    def _log_event(self, **kwargs):
        self.logs.append(kwargs)


class Stage1ApiHandlerTests(unittest.TestCase):
    def _build_store_helpers(self, store_path: pathlib.Path):
        def default_store_factory():
            return app_store.default_store_payload(
                default_notification_config=dev_server._default_notification_config,
                default_bot_config=dev_server._default_bot_config,
                default_ai_config=dev_server._default_ai_config,
                default_cover_studio_config=dev_server._default_cover_studio_config,
                default_drive115_config=dev_server._default_drive115_config,
                default_hdhive_config=dev_server._default_hdhive_config,
                default_library_directory_config=dev_server._default_library_directory_config,
                sync_notification_config_to_bot_config=dev_server.sync_notification_config_to_bot_config,
            )

        def read_store():
            return app_store.read_store_unlocked(
                path=store_path,
                default_store_factory=default_store_factory,
                normalize_bot_config=dev_server._normalize_bot_config,
                normalize_notification_config=dev_server._normalize_notification_config,
                sync_notification_config_to_bot_config=dev_server.sync_notification_config_to_bot_config,
                normalize_ai_config=dev_server._normalize_ai_config,
                normalize_cover_studio_config=dev_server._normalize_cover_studio_config,
                normalize_drive115_config=dev_server._normalize_drive115_config,
                normalize_hdhive_config=dev_server._normalize_hdhive_config,
                normalize_library_directory_config=dev_server._normalize_library_directory_config,
            )

        def write_store(store):
            app_store.write_store_unlocked(path=store_path, store=store)

        return default_store_factory, read_store, write_store

    def test_app_store_read_preserves_library_directory_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store_path = pathlib.Path(temp_dir) / "store.json"
            store_path.write_text(
                json.dumps(
                    {
                        "embyConfig": {},
                        "invites": [],
                        "botConfig": {},
                        "aiConfig": {},
                        "drive115Config": {},
                        "hdhiveConfig": {},
                        "libraryDirectoryConfig": {
                            "roots": [
                                {
                                    "path": "/Volumes/Media",
                                    "enabled": True,
                                    "categories": [
                                        {"label": "亚洲电影", "aliases": ["韩影"], "path": "电影/亚洲电影"}
                                    ],
                                }
                            ]
                        },
                    }
                ),
                encoding="utf-8",
            )
            _, read_store, _ = self._build_store_helpers(store_path)
            store = read_store()

        self.assertEqual(store["libraryDirectoryConfig"]["roots"][0]["path"], "/Volumes/Media")
        self.assertEqual(store["libraryDirectoryConfig"]["roots"][0]["categories"][0]["path"], "电影/亚洲电影")

    def test_invite_sync_persists_library_directory_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(os.environ, {}, clear=True):
            store_path = pathlib.Path(temp_dir) / "store.json"
            _, read_store, write_store = self._build_store_helpers(store_path)
            lock = threading.Lock()
            handler = _DummyHandler(
                {
                    "embyConfig": {
                        "serverUrl": "https://emby.example/emby",
                        "apiKey": "key",
                        "clientName": "VistaMirror",
                        "tmdbEnabled": True,
                        "tmdbToken": "token",
                    },
                    "invites": [],
                    "libraryDirectoryConfig": {
                        "roots": [
                            {
                                "name": "本地媒体库",
                                "path": "/Volumes/Cloud2NAS/115/媒体库",
                                "enabled": True,
                                "maxDepth": 4,
                                "categories": [
                                    {"label": "亚洲电影", "aliases": ["韩影"], "path": "电影/亚洲电影"}
                                ],
                            }
                        ]
                    },
                }
            )

            invite_sync_handlers.handle_invite_sync(
                handler,
                store_lock=lock,
                read_store=read_store,
                write_store=write_store,
                sanitize_invite_record=dev_server._sanitize_invite_record,
                normalize_library_directory_config=dev_server._normalize_library_directory_config,
                merge_emby_config_for_save=dev_server._merge_emby_config_for_save,
                env_managed_emby_fields=dev_server._env_managed_emby_fields,
                merge_invites=dev_server._merge_invites,
                apply_emby_env_overrides=dev_server._apply_emby_env_overrides,
                env_controlled_fields_payload=dev_server._env_controlled_fields_payload,
            )
            stored = read_store()

        self.assertEqual(handler.responses[0][0], 200)
        self.assertEqual(
            stored["libraryDirectoryConfig"]["roots"][0]["categories"][0]["path"],
            "电影/亚洲电影",
        )
        self.assertEqual(handler.responses[0][1]["libraryDirectoryConfig"]["roots"][0]["path"], "/Volumes/Cloud2NAS/115/媒体库")

    def test_invite_sync_status_returns_library_directory_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(os.environ, {}, clear=True):
            store_path = pathlib.Path(temp_dir) / "store.json"
            _, read_store, write_store = self._build_store_helpers(store_path)
            lock = threading.Lock()
            store = read_store()
            store["libraryDirectoryConfig"] = {
                "roots": [
                    {
                        "path": "/Volumes/Cloud2NAS/115/媒体库",
                        "enabled": True,
                        "categories": [{"label": "亚洲电影", "path": "电影/亚洲电影"}],
                    }
                ]
            }
            write_store(store)
            handler = _DummyHandler()

            invite_sync_handlers.handle_invite_sync_status(
                handler,
                store_lock=lock,
                read_store=read_store,
                apply_emby_env_overrides=dev_server._apply_emby_env_overrides,
                normalize_library_directory_config=dev_server._normalize_library_directory_config,
                env_controlled_fields_payload=dev_server._env_controlled_fields_payload,
                invite_to_public=lambda invite: invite,
                now_iso=lambda: "2026-07-04T00:00:00",
            )

        self.assertEqual(handler.responses[0][0], 200)
        self.assertEqual(
            handler.responses[0][1]["libraryDirectoryConfig"]["roots"][0]["path"],
            "/Volumes/Cloud2NAS/115/媒体库",
        )

    def test_bot_config_save_and_get_keep_notification_sync(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(os.environ, {}, clear=True):
            store_path = pathlib.Path(temp_dir) / "store.json"
            _, read_store, write_store = self._build_store_helpers(store_path)
            lock = threading.Lock()
            bot_config = dev_server._default_bot_config()
            bot_config["telegramToken"] = "123456:bot-token"
            bot_config["telegramChatId"] = "123456"
            save_handler = _DummyHandler({"botConfig": bot_config})

            config_handlers.handle_bot_config_save(
                save_handler,
                store_lock=lock,
                read_store=read_store,
                write_store=write_store,
                validate_bot_config=dev_server._validate_bot_config,
                normalize_bot_config=dev_server._normalize_bot_config,
                env_managed_bot_fields=dev_server._env_managed_bot_fields,
                sync_bot_config_into_notification=dev_server._sync_bot_config_into_notification,
                apply_bot_env_overrides=dev_server._apply_bot_env_overrides,
                apply_notification_env_overrides=dev_server._apply_notification_env_overrides,
                env_controlled_fields_payload=dev_server._env_controlled_fields_payload,
                redact_sensitive=dev_server.redact_sensitive,
                telegram_wakeup=None,
            )

            get_handler = _DummyHandler()
            config_handlers.handle_bot_config_get(
                get_handler,
                store_lock=lock,
                read_store=read_store,
                write_store=write_store,
                store_path=lambda: store_path,
                apply_bot_env_overrides=dev_server._apply_bot_env_overrides,
                apply_notification_env_overrides=dev_server._apply_notification_env_overrides,
                env_controlled_fields_payload=dev_server._env_controlled_fields_payload,
            )

        self.assertEqual(save_handler.responses[0][0], 200)
        self.assertEqual(get_handler.responses[0][1]["botConfig"]["telegramToken"], "123456:bot-token")
        self.assertEqual(
            get_handler.responses[0][1]["notificationConfig"]["channels"]["telegram"]["botToken"],
            "123456:bot-token",
        )

    def test_cover_studio_config_get_returns_template_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store_path = pathlib.Path(temp_dir) / "store.json"
            _, read_store, write_store = self._build_store_helpers(store_path)
            handler = _DummyHandler()

            class _StubCoverStudioService:
                def list_fonts(self):
                    return [{"key": "hiragino", "label": "苹方黑体"}]

                def list_modes(self):
                    return [{"key": "stack_classic", "label": "经典堆叠"}] * 6

                def list_accent_tones(self):
                    return [{"key": "blue", "label": "海蓝"}]

                def list_title_align_options(self):
                    return [{"key": "left", "label": "左对齐"}]

            cover_studio_handlers.handle_cover_studio_config_get(
                handler,
                store_lock=threading.Lock(),
                read_store=read_store,
                write_store=write_store,
                normalize_cover_studio_config=dev_server._normalize_cover_studio_config,
                cover_studio_service=_StubCoverStudioService(),
            )

        self.assertEqual(handler.responses[0][0], 200)
        payload = handler.responses[0][1]
        self.assertEqual(len(payload["modes"]), 6)
        self.assertEqual(payload["accentTones"][0]["key"], "blue")
        self.assertEqual(payload["titleAlignOptions"][0]["key"], "left")

    def test_cover_studio_apply_prefers_virtual_folder_upload_target(self) -> None:
        handler = _DummyHandler({"viewId": "uv-1", "previewToken": "preview-1"})
        lock = threading.Lock()

        def read_store():
            return {
                "embyConfig": {"serverUrl": "https://emby.example", "apiKey": "test-key"},
                "coverStudioConfig": dev_server._default_cover_studio_config(),
            }

        written = {}

        def write_store(store):
            written["store"] = store

        class _StubEmbyService:
            def fetch_user_views(self):
                return [
                    {
                        "id": "uv-1",
                        "name": "国产动漫",
                        "browseId": "uv-1",
                        "uploadTargetId": "vf-1",
                        "userViewId": "uv-1",
                        "virtualFolderId": "vf-1",
                    }
                ]

        class _StubCoverStudioService:
            def backup_and_apply(self, *, config, view_id, upload_view_id=None, preview_token, emby_service):
                self.called = {
                    "view_id": view_id,
                    "upload_view_id": upload_view_id,
                    "preview_token": preview_token,
                }
                return {"ok": True}

            def build_view_status(self, *, view_id, config):
                return {"viewId": view_id}

        stub_cover_service = _StubCoverStudioService()

        cover_studio_handlers.handle_cover_studio_apply(
            handler,
            store_lock=lock,
            read_store=read_store,
            write_store=write_store,
            apply_emby_env_overrides=lambda config: config or {},
            normalize_cover_studio_config=dev_server._normalize_cover_studio_config,
            build_emby_service=lambda config: _StubEmbyService(),
            cover_studio_service=stub_cover_service,
        )

        self.assertEqual(handler.responses[0][0], 200)
        self.assertEqual(stub_cover_service.called["view_id"], "uv-1")
        self.assertEqual(stub_cover_service.called["upload_view_id"], "vf-1")

    def test_ai_config_save_and_get_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(os.environ, {}, clear=True):
            store_path = pathlib.Path(temp_dir) / "store.json"
            _, read_store, write_store = self._build_store_helpers(store_path)
            lock = threading.Lock()
            ai_config = dev_server._default_ai_config()
            ai_config["enabled"] = True
            ai_config["apiKey"] = "sk-test"
            save_handler = _DummyHandler({"aiConfig": ai_config})

            config_handlers.handle_ai_config_save(
                save_handler,
                store_lock=lock,
                read_store=read_store,
                write_store=write_store,
                apply_ai_env_overrides=dev_server._apply_ai_env_overrides,
                env_managed_ai_fields=dev_server._env_managed_ai_fields,
                validate_ai_config=dev_server._validate_ai_config,
                normalize_ai_config=dev_server._normalize_ai_config,
                env_controlled_fields_payload=dev_server._env_controlled_fields_payload,
                redact_sensitive=dev_server.redact_sensitive,
                telegram_wakeup=None,
            )

            get_handler = _DummyHandler()
            config_handlers.handle_ai_config_get(
                get_handler,
                store_lock=lock,
                read_store=read_store,
                write_store=write_store,
                store_path=lambda: store_path,
                apply_ai_env_overrides=dev_server._apply_ai_env_overrides,
                env_controlled_fields_payload=dev_server._env_controlled_fields_payload,
            )

        self.assertEqual(save_handler.responses[0][0], 200)
        self.assertTrue(get_handler.responses[0][1]["aiConfig"]["enabled"])
        self.assertEqual(get_handler.responses[0][1]["aiConfig"]["apiKey"], "sk-test")


if __name__ == "__main__":
    unittest.main()
