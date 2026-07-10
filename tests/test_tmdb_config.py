import os
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch

import dev_server
from backend_modules import telegram_commands


class TmdbConfigTests(unittest.TestCase):
    def test_effective_config_preserves_saved_tmdb_fields(self) -> None:
        config = {
            "serverUrl": "https://emby.example",
            "apiKey": "emby-key",
            "tmdbEnabled": True,
            "tmdbToken": "saved-token",
            "tmdbLanguage": "zh-CN",
            "tmdbRegion": "cn",
        }

        with patch.dict(os.environ, {}, clear=True):
            backend = dev_server._apply_emby_env_overrides(config)
            telegram = telegram_commands._apply_emby_env_overrides(config)

        self.assertTrue(backend["tmdbEnabled"])
        self.assertEqual(backend["tmdbToken"], "saved-token")
        self.assertEqual(backend["tmdbRegion"], "CN")
        self.assertEqual(telegram["tmdbToken"], "saved-token")

    def test_empty_submitted_token_preserves_saved_token(self) -> None:
        merged = dev_server._merge_emby_config_for_save(
            {"tmdbToken": "saved-token", "tmdbLanguage": "zh-CN", "tmdbRegion": "CN"},
            {"tmdbEnabled": True, "tmdbToken": "", "serverUrl": "", "apiKey": ""},
        )

        self.assertEqual(merged["tmdbToken"], "saved-token")
        self.assertTrue(merged["tmdbEnabled"])

    def test_app_tmdb_token_environment_override_wins(self) -> None:
        with patch.dict(os.environ, {"APP_TMDB_TOKEN": "environment-token"}, clear=True):
            backend = dev_server._apply_emby_env_overrides({"tmdbToken": "saved-token"})
            telegram = telegram_commands._apply_emby_env_overrides({"tmdbToken": "saved-token"})

        self.assertEqual(backend["tmdbToken"], "environment-token")
        self.assertTrue(backend["tmdbEnabled"])
        self.assertEqual(telegram["tmdbToken"], "environment-token")

    def test_normalize_library_directory_config_preserves_roots_and_categories(self) -> None:
        normalized = dev_server._normalize_library_directory_config(
            {
                "roots": [
                    {
                        "name": "NAS 媒体库",
                        "path": "/Volumes/Media",
                        "enabled": True,
                        "maxDepth": 5,
                        "categories": [
                            {
                                "label": "亚洲电影",
                                "aliases": ["韩影", "日影"],
                                "path": "电影/亚洲电影",
                            }
                        ],
                    }
                ]
            }
        )

        self.assertEqual(normalized["roots"][0]["path"], "/Volumes/Media")
        self.assertEqual(normalized["roots"][0]["maxDepth"], 5)
        self.assertEqual(normalized["roots"][0]["categories"][0]["aliases"], ["韩影", "日影"])

    def test_read_store_unlocked_keeps_library_directory_config(self) -> None:
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
            with patch.object(dev_server, "_store_path", return_value=store_path):
                store = dev_server._read_store_unlocked()

        self.assertIn("libraryDirectoryConfig", store)
        self.assertEqual(store["libraryDirectoryConfig"]["roots"][0]["path"], "/Volumes/Media")
        self.assertEqual(store["libraryDirectoryConfig"]["roots"][0]["categories"][0]["path"], "电影/亚洲电影")


if __name__ == "__main__":
    unittest.main()
