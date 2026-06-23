import pathlib
import tempfile
import unittest

import hdhive_broker


@unittest.skipIf(hdhive_broker.AES is None, "pycryptodome is installed by the Docker image")
class HDHiveBrokerStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = hdhive_broker.BrokerStore(
            pathlib.Path(self.temp_dir.name) / "broker.sqlite3",
            "test-encryption-key-with-more-than-24-characters",
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_installation_secret_is_hashed_and_invalid_secret_is_rejected(self):
        installation = self.store.register()
        self.store.authenticate(installation["installationId"], installation["installationSecret"])
        with self.assertRaises(hdhive_broker.BrokerError):
            self.store.authenticate(installation["installationId"], "wrong-secret")

        with self.store.connect() as conn:
            row = conn.execute("SELECT secret_hash FROM installations WHERE id=?", (installation["installationId"],)).fetchone()
        self.assertNotEqual(row["secret_hash"], installation["installationSecret"])

    def test_tokens_are_encrypted_and_oauth_state_cannot_be_replayed(self):
        installation = self.store.register()
        session = self.store.create_oauth_session(installation["installationId"])
        tokens = {"access_token": "access-sensitive", "refresh_token": "refresh-sensitive", "scope": hdhive_broker.HDHIVE_SCOPES}
        self.store.finish_oauth(session["state"], tokens, {"username": "tester"})
        with self.assertRaises(hdhive_broker.BrokerError):
            self.store.finish_oauth(session["state"], tokens, {"username": "tester"})

        with self.store.connect() as conn:
            row = conn.execute("SELECT token_blob FROM grants WHERE installation_id=?", (installation["installationId"],)).fetchone()
        self.assertNotIn("access-sensitive", row["token_blob"])
        self.assertEqual(self.store.grant(installation["installationId"])["tokens"]["access_token"], "access-sensitive")


if __name__ == "__main__":
    unittest.main()
