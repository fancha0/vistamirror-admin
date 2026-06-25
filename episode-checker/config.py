"""Configuration helpers for the standalone episode checker CLI."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


DEFAULT_VIDEO_EXTENSIONS = (".mkv", ".mp4", ".avi", ".rmvb", ".ts")
DEFAULT_EXCLUDE_KEYWORDS = ("预告", "特典", "花絮", "PV", "NCOP", "NCED")
REPO_ROOT = Path(__file__).resolve().parents[1]


class ConfigError(RuntimeError):
    """Raised when user supplied configuration is incomplete or invalid."""


@dataclass(frozen=True)
class CheckerConfig:
    """Runtime configuration shared by all checker modules."""

    directory: Path
    name: str
    tmdb_auth_mode: str
    tmdb_credential: str
    tmdb_language: str = "zh-CN"
    tmdb_region: str = "CN"
    exclude_keywords: tuple[str, ...] = DEFAULT_EXCLUDE_KEYWORDS
    video_extensions: tuple[str, ...] = DEFAULT_VIDEO_EXTENSIONS
    season: int | None = None
    include_season_0: bool = False
    verbose: bool = False
    json_output: bool = False
    use_vistamirror_config: bool = False


def split_csv(value: str | Iterable[str] | None) -> tuple[str, ...]:
    """Normalize comma separated CLI/config values into a clean tuple."""

    if value is None:
        return ()
    if isinstance(value, str):
        items = value.split(",")
    else:
        items = list(value)
    return tuple(item.strip() for item in items if str(item).strip())


def load_json_config(path: str | Path | None) -> dict[str, Any]:
    """Load an optional JSON config file."""

    if not path:
        return {}
    config_path = Path(path).expanduser()
    if not config_path.exists():
        raise ConfigError(f"配置文件不存在：{config_path}")
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"配置文件不是有效 JSON：{exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError("配置文件根节点必须是 JSON 对象。")
    return data


def infer_name_from_dir(directory: Path) -> str:
    """Infer a series name from the final directory segment."""

    name = directory.expanduser().resolve().name.strip()
    if not name:
        raise ConfigError("无法从目录名推断剧名，请使用 --name 指定。")
    return name


def _merge_tuple(
    cli_value: str | None,
    config_value: str | Iterable[str] | None,
    default_value: Iterable[str],
) -> tuple[str, ...]:
    if cli_value:
        return split_csv(cli_value)
    if config_value:
        return split_csv(config_value)
    return tuple(default_value)


def load_vistamirror_tmdb_config(repo_root: Path | None = None) -> dict[str, str]:
    """Read the saved TMDB settings from the local Vistamirror runtime store."""

    root = repo_root or REPO_ROOT
    candidates = (
        root / "data" / "invites.json",
        root / "invites.json",
        root / "invite_store.json",
    )
    for path in candidates:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise ConfigError(f"Vistamirror 配置文件无法读取：{path}") from exc
        if not isinstance(payload, dict):
            continue
        emby = payload.get("embyConfig")
        if not isinstance(emby, dict):
            continue
        token = str(emby.get("tmdbToken") or os.environ.get("APP_TMDB_TOKEN") or os.environ.get("TMDB_TOKEN") or "").strip()
        language = str(emby.get("tmdbLanguage") or "zh-CN").strip() or "zh-CN"
        region = str(emby.get("tmdbRegion") or "CN").strip().upper() or "CN"
        if not token:
            raise ConfigError("已启用 --use-vistamirror-config，但当前项目中未找到已保存的 TMDB Bearer Token。")
        return {
            "token": token,
            "language": language,
            "region": region,
            "path": str(path),
        }
    raise ConfigError("已启用 --use-vistamirror-config，但未找到 Vistamirror 配置文件。")


def build_config(args: Any) -> CheckerConfig:
    """Build a validated config from argparse args and optional JSON config."""

    file_config = load_json_config(getattr(args, "config", None))

    raw_dir = getattr(args, "directory", None) or file_config.get("dir") or file_config.get("directory")
    if not raw_dir:
        raise ConfigError("缺少扫描目录，请使用 --dir 指定。")
    directory = Path(raw_dir).expanduser()
    if not directory.exists() or not directory.is_dir():
        raise ConfigError(f"扫描目录不存在或不是目录：{directory}")

    name = getattr(args, "name", None) or file_config.get("name") or infer_name_from_dir(directory)
    use_vistamirror_config = bool(
        getattr(args, "use_vistamirror_config", False) or file_config.get("use_vistamirror_config", False)
    )

    tmdb_api_key = os.environ.get("TMDB_API_KEY", "").strip()
    tmdb_auth_mode = ""
    tmdb_credential = ""
    tmdb_language = "zh-CN"
    tmdb_region = "CN"
    if tmdb_api_key:
        tmdb_auth_mode = "api_key"
        tmdb_credential = tmdb_api_key
    elif use_vistamirror_config:
        tmdb_config = load_vistamirror_tmdb_config()
        tmdb_auth_mode = "bearer"
        tmdb_credential = tmdb_config["token"]
        tmdb_language = tmdb_config["language"]
        tmdb_region = tmdb_config["region"]
    else:
        raise ConfigError("缺少 TMDB_API_KEY；如要复用当前项目配置，请显式加上 --use-vistamirror-config。")

    season = getattr(args, "season", None)
    if season is None:
        season = file_config.get("season")
    if season is not None:
        try:
            season = int(season)
        except (TypeError, ValueError) as exc:
            raise ConfigError("--season 必须是数字。") from exc
        if season < 0:
            raise ConfigError("--season 不能小于 0。")

    include_season_0 = bool(getattr(args, "include_season_0", False) or file_config.get("include_season_0", False))
    verbose = bool(getattr(args, "verbose", False) or file_config.get("verbose", False))
    json_output = bool(getattr(args, "json_output", False) or file_config.get("json", False))

    return CheckerConfig(
        directory=directory,
        name=str(name).strip(),
        tmdb_auth_mode=tmdb_auth_mode,
        tmdb_credential=tmdb_credential,
        tmdb_language=tmdb_language,
        tmdb_region=tmdb_region,
        exclude_keywords=_merge_tuple(getattr(args, "exclude", None), file_config.get("exclude_keywords"), DEFAULT_EXCLUDE_KEYWORDS),
        video_extensions=_merge_tuple(None, file_config.get("video_extensions"), DEFAULT_VIDEO_EXTENSIONS),
        season=season,
        include_season_0=include_season_0,
        verbose=verbose,
        json_output=json_output,
        use_vistamirror_config=use_vistamirror_config,
    )
