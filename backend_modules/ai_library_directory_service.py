from __future__ import annotations

import pathlib
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .ai_media_service_adapter import AIMediaServiceAdapter


class AILibraryDirectoryService:
    def __init__(self, media_service: "AIMediaServiceAdapter", *, store_reader=None) -> None:
        self.media_service = media_service
        self._store_reader = store_reader or (lambda: {})

    def fetch_category_items(self, *, spec: dict[str, Any], limit: int = 30) -> tuple[list[dict[str, Any]], int, str, str]:
        query_mode = str(spec.get("queryMode") or "").strip().lower()
        local_rows = self._fetch_from_local_directories(spec=spec, limit=limit)
        if local_rows:
            return local_rows[: max(1, min(50, int(limit or 30)))], len(local_rows), "filesystem", ""
        library_rows = self._fetch_from_named_libraries(spec=spec, limit=limit)
        if library_rows:
            return library_rows[: max(1, min(50, int(limit or 30)))], len(library_rows), "library", ""
        if query_mode == "directory_strict":
            label = str(spec.get("label") or "该分类").strip() or "该分类"
            return [], 0, "", f"未配置目录分类：{label}\n未找到对应目录或库节点，请先配置 libraryDirectoryConfig.categories。"
        fallback_rows = self._fetch_from_recursive_items(spec=spec)
        return fallback_rows[: max(1, min(50, int(limit or 30)))], len(fallback_rows), "metadata", ""

    def _fetch_from_local_directories(self, *, spec: dict[str, Any], limit: int) -> list[dict[str, Any]]:
        config = self._normalize_directory_config(self._read_directory_config())
        roots = config.get("roots") if isinstance(config.get("roots"), list) else []
        query_mode = str(spec.get("queryMode") or "").strip().lower()
        if not roots:
            return []
        matched_dirs: list[pathlib.Path] = []
        for root_spec in roots:
            root_path = self._root_path(root_spec)
            if not root_path:
                continue
            configured_dirs = self._resolve_configured_category_directories(root_spec, spec=spec)
            if configured_dirs:
                matched_dirs.extend(configured_dirs)
                continue
            matched_dirs.extend(self._resolve_structured_category_directories(root_spec, spec=spec))
            if query_mode != "directory_strict":
                matched_dirs.extend(
                    self._find_matching_directories(
                        root_path,
                        spec=spec,
                        max_depth=int(root_spec.get("maxDepth") or config.get("defaultMaxDepth") or 3),
                    )
                )
        if not matched_dirs:
            return []
        unique_dirs: list[pathlib.Path] = []
        seen = set()
        for path in matched_dirs:
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            unique_dirs.append(path)
        rows: list[dict[str, Any]] = []
        row_seen: set[str] = set()
        for directory in unique_dirs:
            try:
                children = sorted(directory.iterdir(), key=lambda item: item.name.lower())
            except Exception:
                continue
            for child in children:
                if child.name.startswith("."):
                    continue
                row = self._build_filesystem_item(child, spec=spec)
                item_key = f"{row.get('Path')}::{row.get('Name')}"
                if item_key in row_seen:
                    continue
                row_seen.add(item_key)
                rows.append(row)
                if len(rows) >= max(1, min(200, int(limit or 30) * 3)):
                    return rows
        return rows

    def _fetch_from_named_libraries(self, *, spec: dict[str, Any], limit: int) -> list[dict[str, Any]]:
        libraries = self.media_service.fetch_emby_libraries()
        if not libraries:
            return []
        matched_libraries = self._match_libraries(libraries, spec=spec)
        if not matched_libraries:
            return []
        include_types = str(spec.get("includeTypes") or "Series,Movie")
        target_limit = max(20, min(200, int(limit or 30) * 3))
        rows_by_id: dict[str, dict[str, Any]] = {}
        for library in matched_libraries:
            library_id = str(library.get("id") or "").strip()
            if not library_id:
                continue
            items = self.media_service.fetch_emby_library_items(
                library_id=library_id,
                include_types=include_types,
                limit=target_limit,
            )
            for row in items:
                if not isinstance(row, dict):
                    continue
                item_id = str(row.get("Id") or "").strip()
                key = item_id or f"{row.get('Name')}::{row.get('ProductionYear')}::{row.get('Type')}"
                if key not in rows_by_id:
                    rows_by_id[key] = row
        return list(rows_by_id.values())

    def _fetch_from_recursive_items(self, *, spec: dict[str, Any]) -> list[dict[str, Any]]:
        candidates = self.media_service.fetch_emby_recursive_items(
            include_types=str(spec.get("includeTypes") or "Series,Movie"),
            limit=500,
        )
        matched = [row for row in candidates if self._matches_category_spec(row, spec)]
        if not matched and not spec.get("match"):
            return candidates
        return matched

    @staticmethod
    def _match_libraries(libraries: list[dict[str, str]], *, spec: dict[str, Any]) -> list[dict[str, str]]:
        targets = {value for value in AILibraryDirectoryService._category_targets(spec) if value}
        matched: list[tuple[int, dict[str, str]]] = []
        for library in libraries:
            name = str(library.get("name") or "").strip().lower()
            lib_type = str(library.get("type") or "").strip().lower()
            score = 0
            if name in targets:
                score += 100
            elif any(target == lib_type for target in targets):
                score += 20
            if score > 0:
                matched.append((score, library))
        matched.sort(key=lambda item: item[0], reverse=True)
        return [library for _score, library in matched]

    def _read_directory_config(self) -> dict[str, Any]:
        store = self._store_reader()
        if not isinstance(store, dict):
            return {}
        raw = store.get("libraryDirectoryConfig")
        return raw if isinstance(raw, dict) else {}

    @classmethod
    def _normalize_directory_config(cls, raw: dict[str, Any]) -> dict[str, Any]:
        source = raw if isinstance(raw, dict) else {}
        raw_roots = source.get("roots")
        if not isinstance(raw_roots, list):
            raw_roots = source.get("directories") if isinstance(source.get("directories"), list) else []
        roots = [row for row in (cls._normalize_root_spec(item) for item in raw_roots) if row]
        default_max_depth = cls._coerce_positive_int(source.get("defaultMaxDepth"), fallback=3, maximum=8)
        return {
            "roots": roots,
            "defaultMaxDepth": default_max_depth,
        }

    @classmethod
    def _normalize_root_spec(cls, root_spec: Any) -> dict[str, Any]:
        if isinstance(root_spec, str):
            path = root_spec
            enabled = True
            name = pathlib.Path(path).name
            categories = []
            max_depth = 3
        elif isinstance(root_spec, dict):
            path = str(root_spec.get("path") or "").strip()
            enabled = bool(root_spec.get("enabled", True))
            name = str(root_spec.get("name") or pathlib.Path(path).name).strip()
            raw_categories = root_spec.get("categories")
            categories = [row for row in (cls._normalize_category_spec(item) for item in raw_categories or []) if row] if isinstance(raw_categories, list) else []
            max_depth = cls._coerce_positive_int(root_spec.get("maxDepth"), fallback=3, maximum=8)
        else:
            return {}
        if not path:
            return {}
        return {
            "path": path,
            "name": name or pathlib.Path(path).name,
            "enabled": enabled,
            "categories": categories,
            "maxDepth": max_depth,
        }

    @staticmethod
    def _normalize_category_spec(category_spec: Any) -> dict[str, Any]:
        if isinstance(category_spec, str):
            label = str(category_spec).strip()
            aliases: list[str] = []
            relative_path = ""
        elif isinstance(category_spec, dict):
            label = str(category_spec.get("label") or category_spec.get("name") or "").strip()
            raw_aliases = category_spec.get("aliases")
            aliases = [str(value).strip() for value in raw_aliases if str(value).strip()] if isinstance(raw_aliases, list) else []
            relative_path = str(category_spec.get("path") or category_spec.get("relativePath") or "").strip()
        else:
            return {}
        if not label and not relative_path:
            return {}
        return {
            "label": label,
            "aliases": aliases,
            "path": relative_path,
        }

    @staticmethod
    def _root_path(root_spec: Any) -> pathlib.Path | None:
        if isinstance(root_spec, str):
            root_spec = {"path": root_spec, "enabled": True}
        if not isinstance(root_spec, dict):
            return None
        raw_path = str(root_spec.get("path") or "").strip()
        enabled = bool(root_spec.get("enabled", True))
        if not enabled or not raw_path:
            return None
        path = pathlib.Path(raw_path).expanduser()
        return path if path.exists() and path.is_dir() else None

    @classmethod
    def _resolve_configured_category_directories(cls, root_spec: dict[str, Any], *, spec: dict[str, Any]) -> list[pathlib.Path]:
        root_path = cls._root_path(root_spec)
        categories = root_spec.get("categories") if isinstance(root_spec.get("categories"), list) else []
        if not root_path or not categories:
            return []
        targets = cls._category_targets(spec)
        matched: list[tuple[int, pathlib.Path]] = []
        for category in categories:
            score = cls._score_category_alias(category, targets=targets)
            if score <= 0:
                continue
            relative_path = str(category.get("path") or "").strip()
            category_path = (root_path / relative_path).resolve() if relative_path else None
            if category_path and category_path.exists() and category_path.is_dir():
                matched.append((score, category_path))
                continue
            label = str(category.get("label") or "").strip()
            if label:
                for path in cls._find_matching_directories_static(root_path, targets=[label.lower()], max_depth=int(root_spec.get("maxDepth") or 3)):
                    matched.append((score, path))
        matched.sort(key=lambda item: item[0], reverse=True)
        return [path for _score, path in matched]

    @classmethod
    def _resolve_structured_category_directories(cls, root_spec: dict[str, Any], *, spec: dict[str, Any]) -> list[pathlib.Path]:
        root_path = cls._root_path(root_spec)
        if not root_path:
            return []
        matched: list[pathlib.Path] = []
        seen: set[str] = set()
        for relative_path in cls._structured_relative_paths(spec):
            candidate = (root_path / relative_path).resolve()
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            if candidate.exists() and candidate.is_dir():
                matched.append(candidate)
        return matched

    def _find_matching_directories(self, root: pathlib.Path, *, spec: dict[str, Any], max_depth: int = 3) -> list[pathlib.Path]:
        return self._find_matching_directories_static(
            root,
            targets=self._category_targets(spec),
            max_depth=max_depth,
        )

    @classmethod
    def _structured_relative_paths(cls, spec: dict[str, Any]) -> list[pathlib.Path]:
        label = str(spec.get("label") or "").strip()
        matched_needle = str(spec.get("matchedNeedle") or "").strip()
        include_types = str(spec.get("includeTypes") or "").strip().lower()

        def add_paths(container: list[pathlib.Path], *segments: str) -> None:
            clean_segments = [str(segment).strip() for segment in segments if str(segment).strip()]
            if clean_segments:
                container.append(pathlib.Path(*clean_segments))

        candidates: list[pathlib.Path] = []
        if label == "亚洲电影":
            sub_labels = cls._structured_subcategory_names(spec)
            for name in sub_labels:
                add_paths(candidates, "电影", "亚洲电影", name)
                add_paths(candidates, "亚洲电影", name)
                add_paths(candidates, name)
            add_paths(candidates, "电影", "亚洲电影")
            add_paths(candidates, "亚洲电影")
        elif label == "华语电影":
            for name in ["华语电影", "国产电影", "中文电影"]:
                add_paths(candidates, "电影", name)
                add_paths(candidates, name)
        elif label == "国产动漫":
            for name in ["国产动漫", "国漫", "中国动漫", "华语动漫"]:
                add_paths(candidates, "动漫", name)
                add_paths(candidates, "动画", name)
                add_paths(candidates, "剧集", name)
                add_paths(candidates, name)
        elif label == "动漫剧集":
            for name in ["动漫剧集", "动画剧集"]:
                add_paths(candidates, "剧集", name)
                add_paths(candidates, "动漫", name)
                add_paths(candidates, name)
            add_paths(candidates, "动漫")
            add_paths(candidates, "动画")
        elif label == "动漫":
            for name in ["动漫", "动画"]:
                add_paths(candidates, name)
                add_paths(candidates, "剧集", name)
        elif label == "纪录片":
            for name in ["纪录片", "Documentary", "documentary"]:
                add_paths(candidates, "电影", name)
                add_paths(candidates, "剧集", name)
                add_paths(candidates, name)
        elif label == "电影":
            for name in ["电影", "Movies", "movies"]:
                add_paths(candidates, name)
        elif label == "剧集":
            for name in ["剧集", "电视剧", "连续剧", "Series", "series"]:
                add_paths(candidates, name)

        if not candidates:
            if "movie" in include_types:
                for target in cls._exact_target_names(spec):
                    add_paths(candidates, "电影", target)
                    add_paths(candidates, target)
            elif "series" in include_types:
                for target in cls._exact_target_names(spec):
                    add_paths(candidates, "剧集", target)
                    add_paths(candidates, target)

        prioritized: list[pathlib.Path] = []
        seen: set[str] = set()
        if matched_needle:
            aliases = cls._structured_alias_expansions(matched_needle)
            for alias in aliases:
                direct = pathlib.Path(alias)
                if str(direct) not in seen:
                    prioritized.append(direct)
                    seen.add(str(direct))
                for prefix in ("电影", "亚洲电影", pathlib.Path("电影") / "亚洲电影"):
                    candidate = pathlib.Path(prefix) / alias if not isinstance(prefix, pathlib.Path) else prefix / alias
                    key = str(candidate)
                    if key not in seen:
                        prioritized.append(candidate)
                        seen.add(key)
        merged: list[pathlib.Path] = []
        for candidate in [*prioritized, *candidates]:
            key = str(candidate)
            if key in seen and candidate not in prioritized:
                continue
            if key not in {str(item) for item in merged}:
                merged.append(candidate)
        return merged

    @staticmethod
    def _find_matching_directories_static(root: pathlib.Path, *, targets: list[str], max_depth: int = 3) -> list[pathlib.Path]:
        if not targets:
            return []
        matched: list[pathlib.Path] = []
        queue: list[tuple[pathlib.Path, int]] = [(root, 0)]
        while queue:
            current, depth = queue.pop(0)
            name = current.name.lower()
            if any(target in name for target in targets):
                matched.append(current)
            if depth >= max_depth:
                continue
            try:
                children = sorted((child for child in current.iterdir() if child.is_dir()), key=lambda item: item.name.lower())
            except Exception:
                continue
            for child in children:
                if child.name.startswith("."):
                    continue
                queue.append((child, depth + 1))
        return matched

    @staticmethod
    def _build_filesystem_item(path: pathlib.Path, *, spec: dict[str, Any]) -> dict[str, Any]:
        title = path.stem if path.is_file() else path.name
        title = re.sub(r"\[[^\]]+\]|\{[^}]+\}", " ", title)
        title = re.sub(r"\s+", " ", title).strip()
        year_match = re.search(r"(19|20)\d{2}", path.name)
        include_types = str(spec.get("includeTypes") or "").lower()
        item_type = "Movie" if include_types == "movie" or include_types == "movie," or include_types == "movie " else ""
        if not item_type:
            item_type = "Series" if "series" in include_types and "movie" not in include_types else "Folder"
        return {
            "Name": title or path.name,
            "Type": item_type,
            "ProductionYear": int(year_match.group(0)) if year_match else 0,
            "Path": str(path),
            "Genres": [],
            "Tags": [],
            "Studios": [],
        }

    @staticmethod
    def _matches_category_spec(item: dict[str, Any], spec: dict[str, Any]) -> bool:
        match_words = [str(word).lower() for word in spec.get("match", []) if str(word).strip()]
        prefer_words = [str(word).lower() for word in spec.get("prefer", []) if str(word).strip()]
        if not match_words and not prefer_words:
            return True
        haystack_parts: list[str] = [
            str(item.get("Name") or ""),
            str(item.get("SeriesName") or ""),
            str(item.get("Type") or ""),
            str(item.get("Path") or ""),
            str(item.get("Overview") or ""),
        ]
        for key in ("Genres", "Tags", "Studios"):
            values = item.get(key)
            if isinstance(values, list):
                haystack_parts.extend(str(value) for value in values)
        provider_ids = item.get("ProviderIds") if isinstance(item.get("ProviderIds"), dict) else {}
        haystack_parts.extend(str(value) for value in provider_ids.values())
        haystack = " ".join(haystack_parts).lower()
        has_match = not match_words or any(word in haystack for word in match_words)
        has_prefer = not prefer_words or any(word in haystack for word in prefer_words)
        return has_match and has_prefer

    @staticmethod
    def _category_targets(spec: dict[str, Any]) -> list[str]:
        needles = [str(value).strip().lower() for value in spec.get("needles", []) if str(value).strip()]
        label = str(spec.get("label") or "").strip().lower()
        return [value for value in [label, *needles] if value]

    @staticmethod
    def _exact_target_names(spec: dict[str, Any]) -> list[str]:
        values = [
            str(spec.get("matchedNeedle") or "").strip(),
            str(spec.get("label") or "").strip(),
            *[str(value).strip() for value in spec.get("needles", []) if str(value).strip()],
        ]
        exact: list[str] = []
        seen: set[str] = set()
        for value in values:
            if not value:
                continue
            lowered = value.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            exact.append(value)
        return exact

    @classmethod
    def _structured_subcategory_names(cls, spec: dict[str, Any]) -> list[str]:
        matched_needle = str(spec.get("matchedNeedle") or "").strip()
        if matched_needle in {"韩影", "韩国电影"}:
            return ["韩国电影"]
        if matched_needle in {"日影", "日本电影"}:
            return ["日本电影"]
        if matched_needle in {"日韩电影"}:
            return ["韩国电影", "日本电影"]
        return []

    @staticmethod
    def _structured_alias_expansions(value: str) -> list[str]:
        text = str(value or "").strip()
        if not text:
            return []
        alias_map = {
            "韩影": ["韩国电影"],
            "韩国电影": ["韩国电影"],
            "日影": ["日本电影"],
            "日本电影": ["日本电影"],
            "日韩电影": ["韩国电影", "日本电影"],
        }
        expansions = alias_map.get(text, [text])
        return [item for item in expansions if str(item).strip()]

    @classmethod
    def _score_category_alias(cls, category: dict[str, Any], *, targets: list[str]) -> int:
        label = str(category.get("label") or "").strip().lower()
        aliases = [str(value).strip().lower() for value in category.get("aliases", []) if str(value).strip()]
        values = [value for value in [label, *aliases] if value]
        if not values or not targets:
            return 0
        score = 0
        for value in values:
            for target in targets:
                if value == target:
                    score += 100
                elif value in target or target in value:
                    score += 60
        return score

    @staticmethod
    def _coerce_positive_int(value: Any, *, fallback: int = 0, maximum: int = 0) -> int:
        try:
            number = int(value)
        except (TypeError, ValueError):
            number = int(fallback or 0)
        if number <= 0:
            number = int(fallback or 0)
        if maximum > 0:
            number = min(number, maximum)
        return number
