import os
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


if __name__ == "__main__":
    unittest.main()
