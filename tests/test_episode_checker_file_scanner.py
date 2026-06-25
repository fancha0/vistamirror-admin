from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest


TOOL_DIR = pathlib.Path(__file__).resolve().parents[1] / "episode-checker"
sys.path.insert(0, str(TOOL_DIR))

from file_scanner import parse_episode_from_name, scan_directory  # noqa: E402


class EpisodeFileScannerTest(unittest.TestCase):
    def test_parse_common_chinese_episode_names(self) -> None:
        cases = {
            "仙逆 - S01E01.mkv": (1, 1),
            "仙逆.E02.mkv": (1, 2),
            "仙逆 第03集.mp4": (1, 3),
            "[SUB]仙逆 04.mkv": (1, 4),
            "仙逆_05.ts": (1, 5),
            "某剧 第2季 第06集.mkv": (2, 6),
            "某剧 - S02E03.mkv": (2, 3),
        }
        for filename, expected in cases.items():
            with self.subTest(filename=filename):
                parsed = parse_episode_from_name(filename)
                self.assertIsNotNone(parsed)
                self.assertEqual((parsed.season, parsed.episode), expected)

    def test_scan_directory_skips_excluded_and_deduplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            for name in [
                "仙逆 - S01E01.mkv",
                "仙逆.E02.mkv",
                "仙逆 第03集.mp4",
                "[SUB]仙逆 04.mkv",
                "仙逆_05.ts",
                "仙逆_05.mp4",
                "仙逆 预告.mp4",
                "readme.txt",
            ]:
                (root / name).write_text("", encoding="utf-8")

            result = scan_directory(root, [".mkv", ".mp4", ".ts"], ["预告"])

        self.assertEqual(result.episodes_by_season[1], {1, 2, 3, 4, 5})
        self.assertEqual(len([item for item in result.records if item.status == "parsed"]), 6)
        self.assertEqual(len([item for item in result.records if item.status == "skipped"]), 1)


if __name__ == "__main__":
    unittest.main()
