"""Command line entry point for the standalone episode checker."""

from __future__ import annotations

import argparse
import sys

from checker import EpisodeChecker, format_text_result, result_to_json
from config import ConfigError, build_config
from tmdb_client import TMDBClient, TMDBError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="独立影视缺失集数检测工具")
    parser.add_argument("--dir", dest="directory", required=True, help="需要递归扫描的视频目录")
    parser.add_argument("--name", help="剧名；不填时使用目录名")
    parser.add_argument("--season", type=int, help="只检测指定季，例如 --season 2")
    parser.add_argument("--config", help="可选 JSON 配置文件")
    parser.add_argument("--exclude", help="逗号分隔排除关键词，例如 预告,特典,PV")
    parser.add_argument("--verbose", action="store_true", help="显示每个文件的解析结果")
    parser.add_argument("--json", dest="json_output", action="store_true", help="输出结构化 JSON")
    parser.add_argument("--include-season-0", action="store_true", help="包含 Season 0 特别篇")
    parser.add_argument(
        "--use-vistamirror-config",
        action="store_true",
        help="当未设置 TMDB_API_KEY 时，改为读取当前仓库里已保存的 TMDB Bearer Token",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = build_config(args)
        client = TMDBClient(
            config.tmdb_credential,
            auth_mode=config.tmdb_auth_mode,
            language=config.tmdb_language,
            region=config.tmdb_region,
        )
        result = EpisodeChecker(config, client).run()
    except (ConfigError, TMDBError) as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("已取消。", file=sys.stderr)
        return 130

    if config.json_output:
        print(result_to_json(result))
    else:
        print(format_text_result(result, verbose=config.verbose, base_dir=config.directory))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
