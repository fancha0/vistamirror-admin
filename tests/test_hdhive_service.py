import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch
import urllib.parse

from backend_modules.hdhive_service import (
    HDHiveService,
    merge_hdhive_config_for_save,
    public_hdhive_config,
)
from backend_modules.telegram_commands import TelegramCommandService


class HDHiveServiceTests(unittest.TestCase):
    def test_public_config_never_exposes_secret_or_tokens(self):
        payload = public_hdhive_config(
            {
                "enabled": True,
                "clientId": "app_test",
                "appSecret": "super-secret-value",
                "accessToken": "access-secret",
                "refreshToken": "refresh-secret",
            }
        )
        self.assertTrue(payload["hasAppSecret"])
        self.assertTrue(payload["authorized"])
        self.assertNotIn("appSecret", payload)
        self.assertNotIn("accessToken", payload)
        self.assertNotIn("refreshToken", payload)

    def test_blank_secret_keeps_saved_secret(self):
        saved = merge_hdhive_config_for_save(
            {"clientId": "app_test", "appSecret": "saved-secret"},
            {"enabled": True, "clientId": "app_test", "appSecret": ""},
        )
        self.assertEqual(saved["appSecret"], "saved-secret")

    def test_authorize_url_contains_fixed_scopes_and_state(self):
        service = HDHiveService({"clientId": "app_test", "appSecret": "secret"})
        url = service.build_authorize_url(state="state-value", redirect_uri="http://localhost:8091/api/hdhive/oauth/callback")
        parsed = urllib.parse.urlsplit(url)
        params = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(parsed.path, "/openapi/authorize")
        self.assertEqual(params["client_id"], ["app_test"])
        self.assertEqual(params["scope"], ["meta query unlock write"])
        self.assertEqual(params["state"], ["state-value"])
        self.assertEqual(params["response_mode"], ["redirect"])

    def test_broker_public_config_hides_installation_secret(self):
        payload = public_hdhive_config(
            {
                "enabled": True,
                "authMode": "broker",
                "brokerUrl": "https://broker.example.com",
                "installationId": "ins_public",
                "installationSecret": "installation-secret",
                "user": {"username": "tester"},
            }
        )
        self.assertTrue(payload["registered"])
        self.assertTrue(payload["authorized"])
        self.assertNotIn("installationSecret", payload)

    def test_existing_direct_credentials_keep_direct_mode(self):
        service = HDHiveService({"clientId": "app_test", "appSecret": "secret", "accessToken": "token"})
        self.assertFalse(service.is_broker)

    def test_broker_registration_and_oauth_session_are_persisted(self):
        saved = []
        service = HDHiveService({"enabled": True, "authMode": "broker", "brokerUrl": "https://broker.example.com"}, save_config=lambda value: saved.append(dict(value)))

        def fake_request(path, **kwargs):
            if path == "/v1/installations/register":
                return {"ok": True, "installationId": "ins_test", "installationSecret": "install-secret"}
            if path == "/v1/oauth/sessions":
                return {"ok": True, "sessionId": "oas_test", "authorizeUrl": "https://hdhive.com/authorize", "expiresAt": 12345}
            raise AssertionError(path)

        service._broker_request = fake_request
        session = service.create_broker_oauth_session()
        self.assertEqual(session["sessionId"], "oas_test")
        self.assertEqual(service.config["installationId"], "ins_test")
        self.assertEqual(service.config["oauthSessionId"], "oas_test")
        self.assertGreaterEqual(len(saved), 2)


class FakeIdentityService:
    def resolve(self, keyword):
        return {
            "identity": {"title": "遮天", "year": "2023", "type": "series", "tmdbId": "224839"},
            "embyItem": {},
            "candidates": [],
            "ambiguous": False,
        }


class FakeHDHiveService:
    def __init__(self, config, save_config=None):
        self.config = {"enabled": True, "accessToken": "token", **config}

    @property
    def is_broker(self):
        return False

    def search_resources(self, **kwargs):
        return {
            "items": [
                {"slug": "slug-115", "title": "遮天 4K", "pan_type": "115", "share_size": "10GB", "video_resolution": ["2160p"], "unlock_points": 3},
                {"slug": "slug-other", "title": "遮天 夸克", "pan_type": "quark", "share_size": "8GB", "unlock_points": 1},
            ]
        }


class TelegramHDHiveTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.temp_dir.name)
        self.store_path = root / "invites.json"
        self.store_path.write_text(
            json.dumps(
                {
                    "hdhiveConfig": {"enabled": True, "clientId": "app", "appSecret": "secret", "accessToken": "token"},
                    "drive115Config": {"enabled": True, "cookie": "cookie", "defaultCid": "100"},
                }
            ),
            encoding="utf-8",
        )
        self.service = TelegramCommandService(store_path=self.store_path, event_log_path=root / "events.jsonl")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_search_lists_resources_but_only_115_gets_transfer_button(self):
        self.service._media_identity_service = lambda: FakeIdentityService()
        with patch("backend_modules.telegram_commands.HDHiveService", FakeHDHiveService):
            reply = self.service._cmd_hdhive_search("遮天")

        self.assertIn("遮天 4K", reply["fallback_text"])
        self.assertIn("遮天 夸克", reply["fallback_text"])
        buttons = reply["reply_markup"]["inline_keyboard"]
        self.assertEqual(len(buttons), 1)
        self.assertTrue(buttons[0][0]["callback_data"].startswith("hdhive:pick:"))


if __name__ == "__main__":
    unittest.main()
