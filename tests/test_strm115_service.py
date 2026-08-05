import tempfile
import threading
import unittest
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

    def test_emby_web_proxy_normalizes_api_base_without_losing_subpath(self):
        self.assertEqual("http://emby:8096", dev_server._normalize_emby_web_proxy_base("http://emby:8096/emby"))
        self.assertEqual("https://media.example.test", dev_server._normalize_emby_web_proxy_base("https://media.example.test/"))
        self.assertEqual("https://proxy.example.test/media", dev_server._normalize_emby_web_proxy_base("https://proxy.example.test/media/emby"))
        self.assertEqual("", dev_server._normalize_emby_web_proxy_base("emby:8096"))

    def test_dedicated_playback_port_proxies_emby_web_root(self):
        class UpstreamHandler(BaseHTTPRequestHandler):
            def do_GET(self):
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
        finally:
            playback.shutdown()
            playback.server_close()
            upstream.shutdown()
            upstream.server_close()


if __name__ == "__main__":
    unittest.main()
