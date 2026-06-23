import unittest
from unittest.mock import patch

from backend_modules.drive115_service import Drive115Error, Drive115Service


class Drive115TransferTests(unittest.TestCase):
    def make_service(self):
        return Drive115Service({"enabled": True, "cookie": "UID=test; CID=test", "defaultCid": "100"})

    def test_submit_uses_single_file_id_and_user_id(self):
        service = self.make_service()
        requests = []

        def fake_request(url, *, method="GET", data=None, timeout=None):
            requests.append((url, method, data))
            if "get_user_aq" in url:
                return {"state": True, "user_id": "7788"}
            return {"state": True, "data": {"received": True}}

        service._request = fake_request
        with patch("backend_modules.drive115_service.time.sleep"):
            result = service.transfer_share(
                share_code="abc123",
                receive_code="p1a5",
                target_cid="100",
                file_ids=["file-1"],
                source_files=[{"name": "episode.mkv", "size": 1024, "isDir": False}],
            )

        receive = next(row for row in requests if "share/receive" in row[0])
        self.assertEqual(receive[2]["user_id"], "7788")
        self.assertEqual(receive[2]["file_id"], "file-1")
        self.assertNotIn("file_id[]", receive[2])
        self.assertEqual(result["status"], "submitted")

    def test_false_duplicate_refreshes_ids_and_retries(self):
        service = self.make_service()
        receive_calls = []

        def fake_request(url, *, method="GET", data=None, timeout=None):
            if "get_user_aq" in url:
                return {"state": True, "user_id": "7788"}
            if "share/receive" in url:
                receive_calls.append(dict(data or {}))
                if len(receive_calls) == 1:
                    return {"state": False, "errno": 4100008, "error": "你已经转存过该文件"}
                return {"state": True, "data": {"received": True}}
            if "share/snap" in url:
                return {"state": True, "data": {"list": [{"fid": "fresh-id", "n": "episode.mkv", "s": 1024}]}}
            if "webapi.115.com/files?" in url:
                return {"state": True, "data": {"data": []}}
            raise AssertionError(url)

        service._request = fake_request
        with patch("backend_modules.drive115_service.time.sleep"):
            result = service.transfer_share(
                share_code="abc123",
                target_cid="100",
                file_ids=["stale-id"],
                source_files=[{"name": "episode.mkv", "size": 1024}],
            )

        self.assertEqual(result["status"], "submitted")
        self.assertEqual([row["file_id"] for row in receive_calls], ["stale-id", "fresh-id"])

    def test_real_duplicate_is_reported_as_exists(self):
        service = self.make_service()

        def fake_request(url, *, method="GET", data=None, timeout=None):
            if "get_user_aq" in url:
                return {"state": True, "user_id": "7788"}
            if "share/receive" in url:
                return {"state": False, "errno": 4100008, "error": "你已经转存过该文件"}
            if "webapi.115.com/files?" in url:
                return {"state": True, "data": {"data": [{"n": "episode.mkv", "s": 1024}]}}
            raise AssertionError(url)

        service._request = fake_request
        with patch("backend_modules.drive115_service.time.sleep"):
            result = service.transfer_share(
                share_code="abc123",
                target_cid="100",
                file_ids=["file-1"],
                source_files=[{"name": "episode.mkv", "size": 1024}],
            )

        self.assertEqual(result["status"], "exists")

    def test_same_name_with_different_size_is_not_existing(self):
        service = self.make_service()

        def fake_request(url, *, method="GET", data=None, timeout=None):
            if "get_user_aq" in url:
                return {"state": True, "user_id": "7788"}
            if "share/receive" in url:
                return {"state": False, "errno": 4100008, "error": "你已经转存过该文件"}
            if "share/snap" in url:
                return {"state": True, "data": {"list": [{"fid": "fresh-id", "n": "episode.mkv", "s": 1024}]}}
            if "webapi.115.com/files?" in url:
                return {"state": True, "data": {"data": [{"n": "episode.mkv", "s": 2048}]}}
            raise AssertionError(url)

        service._request = fake_request
        with patch("backend_modules.drive115_service.time.sleep"):
            with self.assertRaisesRegex(Drive115Error, "目标目录未发现相同文件"):
                service.transfer_share(
                    share_code="abc123",
                    target_cid="100",
                    file_ids=["file-1"],
                    source_files=[{"name": "episode.mkv", "size": 1024}],
                )


if __name__ == "__main__":
    unittest.main()
