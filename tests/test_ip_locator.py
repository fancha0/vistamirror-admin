import unittest
from unittest import mock

from backend_modules import ip_locator


class FakeResponse:
    def __init__(self, body: bytes):
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class IpLocatorTests(unittest.TestCase):
    def setUp(self):
        ip_locator._IP_GEO_CACHE.clear()

    def test_build_ip_display_prefers_payload_geo(self):
        payload = {
            "RemoteEndPoint": "58.23.230.211:0",
            "Geo": "福建 厦门",
        }
        self.assertEqual(
            ip_locator.build_ip_display(payload, show_ip=True, show_geo=True),
            "58.23.230.211 福建 厦门",
        )

    @mock.patch("backend_modules.ip_locator.urllib.request.urlopen")
    def test_build_ip_display_looks_up_public_ip_geo(self, mock_urlopen):
        mock_urlopen.return_value = FakeResponse(b'{"pro":"\xe7\xa6\x8f\xe5\xbb\xba\xe7\x9c\x81","city":"\xe5\x8e\xa6\xe9\x97\xa8\xe5\xb8\x82"}')
        payload = {
            "Session": {"RemoteEndPoint": "58.23.230.211"},
        }
        self.assertEqual(
            ip_locator.build_ip_display(payload, show_ip=True, show_geo=True),
            "58.23.230.211 福建 厦门",
        )

    @mock.patch("backend_modules.ip_locator.urllib.request.urlopen")
    def test_build_ip_display_skips_lookup_for_private_ip(self, mock_urlopen):
        payload = {
            "RemoteEndPoint": "192.168.1.8:8096",
        }
        self.assertEqual(
            ip_locator.build_ip_display(payload, show_ip=True, show_geo=True),
            "192.168.1.8",
        )
        mock_urlopen.assert_not_called()
