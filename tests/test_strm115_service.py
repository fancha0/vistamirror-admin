import tempfile
import threading
import unittest
from unittest.mock import patch
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen

from backend_modules.project_event_logger import append_project_event, read_project_events
from backend_modules.strm115_service import Strm115Service
import dev_server


class FakeDrive:
    def __init__(self):
        self.pages = {
            "99": [{"id": "films", "name": "Films", "isDir": True}, {"id": "shows", "name": "Shows", "isDir": True}],
            "films": [{"id": "movie-1", "name": "Movie.mkv", "pickCode": "pick-1", "sha1": "a1", "size": 10}, {"id": "text-1", "name": "readme.txt", "pickCode": "pick-2", "sha1": "a2", "size": 3}],
            "shows": [{"id": "show", "name": "Show", "isDir": True}],
            "show": [{"id": "episode-1", "name": "E01.mp4", "pickCode": "pick-3", "sha1": "a3", "size": 20}],
        }

    def list_directory_page(self, cid, *, offset=0, limit=200):
        self.cid = cid
        rows = self.pages.get(cid, [])[offset:offset + limit]
        return {"items": rows, "nextOffset": offset + len(rows), "hasMore": offset + len(rows) < len(self.pages.get(cid, []))}

    def list_files_recursive(self, cid):
        self.cid = cid
        return [
            {"id": "movie-1", "name": "Movie.mkv", "path": "Films/Movie.mkv", "pickCode": "pick-1", "sha1": "a1", "size": 10},
            {"id": "text-1", "name": "readme.txt", "path": "Films/readme.txt", "pickCode": "pick-2", "sha1": "a2", "size": 3},
            {"id": "episode-1", "name": "E01.mp4", "path": "Shows/Show/E01.mp4", "pickCode": "pick-3", "sha1": "a3", "size": 20},
        ]


class Strm115ServiceTests(unittest.TestCase):
    def make_service(self, root):
        return Strm115Service(
            {
                "enabled": True,
                "sourceCid": "99",
                "outputDir": str(root / "library"),
                "publicBaseUrl": "https://mirror.example.test",
                "signingSecret": "test-secret",
            },
            root / "index.json",
            FakeDrive(),
        )

    def test_sync_writes_video_strm_and_keeps_stable_signed_url(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = self.make_service(root)
            result = service.sync()
            self.assertTrue(result["ok"])
            self.assertEqual(result["summary"]["videos"], 2)
            self.assertEqual(result["summary"]["created"], 2)
            movie_strm = root / "library" / "Films" / "Movie.strm"
            self.assertTrue(movie_strm.exists())
            content = movie_strm.read_text(encoding="utf-8").strip()
            self.assertIn("/d/movie-1.mkv?exp=0&sig=", content)
            second = service.sync()
            self.assertEqual(second["summary"]["unchanged"], 2)
            record = service.resolve_file("movie-1", expires="0", signature=content.split("sig=", 1)[1])
            self.assertEqual(record["pickCode"], "pick-1")

    def test_named_links_keep_old_signatures_and_encode_special_characters(self):
        with tempfile.TemporaryDirectory() as temporary:
            service = self.make_service(Path(temporary))
            service.sync()
            old = service.signed_stream_url("movie-1")
            service.config["includeFileName"] = True
            named = service.signed_stream_url("movie-1", name="毒蜂 & #.mkv")
            self.assertTrue(named.startswith(old + "&name="))
            self.assertNotIn("毒蜂", named)
            self.assertIn("%26", named)
            self.assertEqual(service.resolve_file("movie-1", expires="0", signature=old.split("sig=")[1])["id"], "movie-1")

    def test_link_format_migration_and_persistence(self):
        from backend_modules.strm115_service import default_strm115_config, normalize_strm115_config, merge_strm115_config, public_strm115_config
        self.assertEqual(default_strm115_config()["linkFormat"], "short_named")
        self.assertEqual(normalize_strm115_config({})["linkFormat"], "signed")
        self.assertEqual(normalize_strm115_config({"includeFileName": True})["linkFormat"], "named")
        config = merge_strm115_config({"signingSecret": "existing"}, {"linkFormat": "short_named"})
        self.assertEqual(normalize_strm115_config(config)["linkFormat"], "short_named")
        self.assertEqual(config["signingSecret"], "existing")
        self.assertNotIn("signingSecret", public_strm115_config(config))

    def test_rewrite_handler_serializes_with_sync_and_reports_partial_failure(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        handler = SimpleNamespace(_read_json_body=lambda: {}, _send_json=Mock(), _log_event=Mock(),
                                  _strm115_service=Mock(return_value=SimpleNamespace(rewrite_links=lambda: {"ok": False, "summary": {"failed": 1}, "errors": []})))
        with patch.object(dev_server, "STRM115_SYNC_LOCK", threading.Lock()):
            dev_server.STRM115_SYNC_LOCK.acquire()
            dev_server.AppHandler._handle_strm115_rewrite_links(handler)
            self.assertEqual(handler._send_json.call_args.args[0], 409)
            handler._strm115_service.assert_not_called()
            dev_server.STRM115_SYNC_LOCK.release()
            dev_server.AppHandler._handle_strm115_rewrite_links(handler)
            status, result = handler._send_json.call_args.args
            self.assertEqual(status, 200)
            self.assertFalse(result["complete"])
            self.assertEqual(result["summary"]["failed"], 1)
            self.assertFalse(dev_server.STRM115_SYNC_LOCK.locked())

    def test_short_named_links_are_stable_encoded_and_old_links_still_work(self):
        from urllib.parse import urlsplit, unquote
        with tempfile.TemporaryDirectory() as temporary:
            service = self.make_service(Path(temporary))
            service.config["requestIntervalMs"] = 0
            service.sync()
            old = service.signed_stream_url("movie-1")
            service.config["linkFormat"] = "short_named"
            url = service.signed_stream_url("movie-1", name="毒蜂 & #? / 2023.mkv")
            parts = urlsplit(url)
            token = parts.path.split("/")[-1].rsplit(".", 1)[0]
            self.assertEqual(len(token), 26)
            self.assertEqual(parts.fragment, "")
            self.assertEqual(unquote(parts.query), "/毒蜂 & #? / 2023.mkv")
            self.assertNotIn("sig=", url)
            self.assertEqual(service.resolve_short_file(token)["id"], "movie-1")
            restarted = Strm115Service(service.config, service.index_path, service.drive)
            self.assertEqual(url, restarted.signed_stream_url("movie-1", name="毒蜂 & #? / 2023.mkv"))
            self.assertEqual(service.resolve_file("movie-1", expires="0", signature=old.split("sig=")[1])["id"], "movie-1")
            with self.assertRaises(RuntimeError):
                service.resolve_short_file("s_" + "x" * 24)
            with self.assertRaises(RuntimeError):
                service.resolve_short_file("s_" + "中" * 24)
            service.config["signingSecret"] = "rotated-secret"
            with self.assertRaises(RuntimeError):
                service.resolve_short_file(token)

    def test_rewrite_uses_only_index_and_preserves_sync_cursor_and_handles_paths(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = self.make_service(root)
            service.config["requestIntervalMs"] = 0
            service.sync()
            index = service._read_index()
            index["syncState"] = {"active": True, "pending": [{"cid": "next"}]}
            before = index["lastSyncedAt"]
            outside = root / "outside.strm"
            outside.write_text("do not touch")
            alias = root / "library" / "alias.strm"
            alias.symlink_to(outside)
            for key, path in [("outside", outside), ("alias", alias), ("missing", root / "library" / "gone.strm")]:
                index["files"][key] = {"id": key, "name": key + ".mkv", "strmPath": str(path)}
            service._write_index(index)
            service.config["linkFormat"] = "short_named"
            with patch.object(service.drive, "list_directory_page", side_effect=AssertionError("must not scan")):
                result = service.rewrite_links()
                again = service.rewrite_links()
            self.assertEqual(result["summary"], {"updated": 2, "unchanged": 0, "missing": 1, "failed": 2})
            self.assertEqual(again["summary"]["unchanged"], 2)
            self.assertEqual(outside.read_text(), "do not touch")
            self.assertIn("?/Movie.mkv", (root / "library" / "Films" / "Movie.strm").read_text())
            self.assertEqual(service._read_index()["syncState"], index["syncState"])
            self.assertEqual(service._read_index()["lastSyncedAt"], before)

    def test_cache_scoped_to_account_and_user_agent_and_force(self):
        with tempfile.TemporaryDirectory() as temporary:
            service = self.make_service(Path(temporary))
            service.drive.cookie = "account-a"
            calls = []
            service.drive.resolve_download_url = lambda code, user_agent="": calls.append((code, user_agent)) or "https://cdn.example/video"
            record = {"pickCode": "cache-test"}
            service.resolve_playback_url(record, user_agent="A")
            self.assertTrue(service.resolve_playback_url(record, user_agent="A")[1])
            self.assertFalse(service.resolve_playback_url(record, user_agent="B")[1])
            service.drive.cookie = "account-b"
            self.assertFalse(service.resolve_playback_url(record, user_agent="A")[1])
            self.assertFalse(service.resolve_playback_url(record, user_agent="A", force=True)[1])
            self.assertEqual(len(calls), 4)

    def test_playback_probe_reads_only_one_kib_and_reports_range(self):
        with tempfile.TemporaryDirectory() as temporary:
            service = self.make_service(Path(temporary))
            service.sync()
            reads = []
            class Response:
                status = 206
                headers = {"Content-Type": "video/x-matroska", "Content-Range": "bytes 0-1023/6000000000"}
                def read(self, size): reads.append(size); return b"x" * size
                def __enter__(self): return self
                def __exit__(self, *args): return False
            with patch.object(service, "resolve_playback_url", return_value=("https://cdn.example/video", False)), patch("backend_modules.strm115_service.urllib.request.urlopen", return_value=Response()) as request:
                result = service.test_playback("movie-1", user_agent="Emby")
                self.assertEqual(request.call_args.args[0].get_header("Range"), "bytes=0-1023")
            self.assertEqual(reads, [1024])
            self.assertTrue(result["rangeSupported"])
            self.assertNotIn("cdn.example", result["streamUrl"])

    def test_rejects_bad_signature_and_root_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            service = self.make_service(Path(temporary))
            service.sync()
            with self.assertRaisesRegex(RuntimeError, "签名无效"):
                service.resolve_file("movie-1", expires="0", signature="not-valid")
            service.config["outputDir"] = "/"
            with self.assertRaisesRegex(RuntimeError, "非根目录"):
                service.validate()

    def test_safe_incremental_resumes_batches_and_only_then_marks_orphans(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = self.make_service(root)
            service.config["maxPagesPerRun"] = 1
            first = service.sync(mode="safe_incremental")
            self.assertFalse(first["summary"]["complete"])
            self.assertEqual(first["summary"]["remainingDirectories"], 2)
            for _ in range(4):
                result = service.sync(mode="safe_incremental")
                if result["summary"]["complete"]:
                    break
            self.assertTrue(result["summary"]["complete"])
            self.assertEqual(service.status()["fileCount"], 2)
            preview = service.cleanup_orphans()
            self.assertEqual(preview["count"], 0)

    def test_strm_events_are_queryable_separately_from_system_events(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "events.jsonl"
            append_project_event(path, module="strm115", action="strm115_playback_redirect", message="115 STRM 播放地址已解析并跳转。")
            append_project_event(path, module="system", action="other", message="其他系统事件")
            events, total = read_project_events(path, module="strm115")
            self.assertEqual(total, 2)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["action"], "strm115_playback_redirect")

    def test_signed_playback_get_and_head_redirect_with_player_user_agent(self):
        with tempfile.TemporaryDirectory() as temporary:
            service = self.make_service(Path(temporary))
            service.sync()
            agents = []
            def resolve(record, *, user_agent="", force=False):
                agents.append(user_agent)
                return "https://cdn.example/video.mkv", False
            service.resolve_playback_url = resolve
            class Handler(dev_server.StrmPlaybackHandler):
                def _strm115_service(self): return service
                def _log_event(self, **kwargs): pass
                def log_message(self, *args): pass
            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            import urllib.request, urllib.error
            class NoRedirect(urllib.request.HTTPRedirectHandler):
                def redirect_request(self, *args): return None
            opener = urllib.request.build_opener(NoRedirect())
            suffix = service.signed_stream_url("movie-1").split("/d/", 1)[1]
            try:
                for method, route in [(method, route) for method in ("GET", "HEAD") for route in (suffix, service._short_token("movie-1") + ".mkv?/%E6%AF%92%E8%9C%82.mkv")]:
                    req = urllib.request.Request(f"http://127.0.0.1:{server.server_port}/d/{route}", method=method, headers={"User-Agent": "Emby-player"})
                    with self.assertRaises(urllib.error.HTTPError) as raised:
                        opener.open(req, timeout=3)
                    response = raised.exception
                    self.assertEqual(response.code, 307)
                    self.assertEqual(response.headers["Location"], "https://cdn.example/video.mkv")
                    self.assertEqual(response.read(), b"")
                    response.close()
                self.assertEqual(agents, ["Emby-player"] * 4)
            finally:
                server.shutdown(); server.server_close(); thread.join()

    def test_emby_web_proxy_normalizes_api_base_without_losing_subpath(self):
        self.assertEqual("http://emby:8096", dev_server._normalize_emby_web_proxy_base("http://emby:8096/emby"))
        self.assertEqual("https://media.example.test", dev_server._normalize_emby_web_proxy_base("https://media.example.test/"))
        self.assertEqual("https://proxy.example.test/media", dev_server._normalize_emby_web_proxy_base("https://proxy.example.test/media/emby"))
        self.assertEqual("", dev_server._normalize_emby_web_proxy_base("emby:8096"))

    def test_dedicated_playback_port_proxies_emby_web_root(self):
        class UpstreamHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                type(self).received_accept_encoding = self.headers.get("Accept-Encoding")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Set-Cookie", "emby-session=test; Path=/")
                self.end_headers()
                self.wfile.write(f"emby:{self.path}".encode("utf-8"))

            def log_message(self, *_args):
                return

        upstream = ThreadingHTTPServer(("127.0.0.1", 0), UpstreamHandler)
        upstream_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        upstream_thread.start()

        class PlaybackHandler(dev_server.StrmPlaybackHandler):
            def _configured_emby_web_base(self):
                return f"http://127.0.0.1:{upstream.server_port}"

        playback = ThreadingHTTPServer(("127.0.0.1", 0), PlaybackHandler)
        playback_thread = threading.Thread(target=playback.serve_forever, daemon=True)
        playback_thread.start()
        try:
            with urlopen(f"http://127.0.0.1:{playback.server_port}/web/index.html?start=1", timeout=5) as response:
                self.assertEqual(response.status, 200)
                self.assertEqual(response.headers.get("Set-Cookie"), "emby-session=test; Path=/")
                self.assertEqual(response.read().decode("utf-8"), "emby:/web/index.html?start=1")
                self.assertEqual(UpstreamHandler.received_accept_encoding, "identity")
        finally:
            playback.shutdown()
            playback.server_close()
            upstream.shutdown()
            upstream.server_close()


if __name__ == "__main__":
    unittest.main()
