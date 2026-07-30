import unittest
from unittest.mock import patch

from backend_modules.drive115_service import Drive115Error, Drive115Service


class Drive115TransferTests(unittest.TestCase):
    def make_service(self):
        return Drive115Service({"enabled": True, "cookie": "UID=test; CID=test", "defaultCid": "100"})

    def test_playback_url_follows_json_file_url_302(self):
        service = self.make_service()
        opened = []

        class Response:
            def __init__(self, body="", location=""):
                self.body = body.encode("utf-8")
                self.headers = {"Location": location}

            def read(self):
                return self.body

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        class Opener:
            def open(self, request, timeout=None):
                opened.append(request)
                if "webapi.115.com/files/download" in request.full_url:
                    return Response('{"state":true,"data":{"file_url_302":"https://bridge.example/once"}}')
                if request.full_url != "https://bridge.example/once":
                    raise AssertionError(request.full_url)
                return Response(location="https://cdn.example/video.mkv")

        with patch("backend_modules.drive115_service.urllib.request.build_opener", return_value=Opener()):
            result = service.resolve_download_url("pick-123")

        self.assertEqual(result, "https://cdn.example/video.mkv")
        self.assertIn("dl=1", opened[0].full_url)
        self.assertIsNone(opened[1].get_header("Cookie"))

    def test_playback_url_surfaces_expired_cookie_response(self):
        service = self.make_service()

        class Response:
            headers = {"Location": ""}

            def read(self):
                return '{"state":false,"errno":990001,"error":"登录超时，请重新登录。"}'.encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        class Opener:
            def open(self, request, timeout=None):
                return Response()

        with patch("backend_modules.drive115_service.urllib.request.build_opener", return_value=Opener()):
            with self.assertRaisesRegex(RuntimeError, "登录超时"):
                service.resolve_download_url("pick-123")

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

    def test_recursive_listing_preserves_relative_path_and_pick_code(self):
        service = self.make_service()

        def fake_request(url, *, method="GET", data=None, timeout=None):
            if "cid=root" in url:
                # Real 115 folder rows currently use fc=0 and omit fid.
                return {"state": True, "data": {"data": [{"cid": "child", "n": "Movies", "fc": 0}]}}
            if "cid=child" in url:
                return {"state": True, "data": {"data": [{"fid": "f1", "n": "Film.mkv", "pc": "pc1", "sha": "sha1", "s": 42}]}}
            raise AssertionError(url)

        service._request = fake_request
        rows = service.list_files_recursive("root")
        self.assertEqual(rows, [{"id": "f1", "name": "Film.mkv", "path": "Movies/Film.mkv", "isDir": False, "pickCode": "pc1", "sha1": "sha1", "size": 42}])


if __name__ == "__main__":
    unittest.main()
