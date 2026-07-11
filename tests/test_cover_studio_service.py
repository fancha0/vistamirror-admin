from __future__ import annotations

from io import BytesIO
import base64
import json
import os
import pathlib
import tempfile
import threading
import unittest
from datetime import datetime

from PIL import Image

from backend_modules.cover_studio_service import (
    CoverStudioService,
    EmbyCoverService,
    available_cover_fonts,
    cover_studio_modes,
    default_cover_studio_config,
    is_valid_cover_studio_cron,
    normalize_cover_studio_config,
)
from backend_modules.cover_studio_scheduler import CoverStudioScheduler, cron_matches
from backend_modules.cover_studio_template_specs import get_cinematic_showcase_variant


class _FakeEmbyService:
    def __init__(self):
        self.uploads = []
        self.deletes = []

    def fetch_primary_image_bytes(self, *, item_id: str, image_tag: str = "", max_width: int = 700) -> bytes:
        image = Image.new("RGB", (420, 630), (48, 96, 180))
        buffer = BytesIO()
        image.save(buffer, format="PNG")
        return buffer.getvalue()

    def fetch_view_image_bytes(self, *, view_id: str, image_type: str) -> bytes | None:
        image = Image.new("RGB", (600, 338), (18, 28, 46))
        buffer = BytesIO()
        image.save(buffer, format="PNG")
        return buffer.getvalue()

    def upload_view_image(self, *, view_id: str, image_type: str, image_bytes: bytes, content_type: str = "image/png") -> None:
        self.uploads.append((view_id, image_type, content_type, len(image_bytes)))

    def delete_view_image(self, *, view_id: str, image_type: str) -> bool:
        self.deletes.append((view_id, image_type))
        return True


class _BackdropEmbyService(_FakeEmbyService):
    def __init__(self):
        super().__init__()
        self.backdrop_requests = []

    def fetch_backdrop_image_bytes(self, *, item_id: str, image_tag: str = "", max_width: int = 1800) -> bytes:
        self.backdrop_requests.append((item_id, image_tag, max_width))
        image = Image.new("RGB", (1600, 900), (132, 34, 22))
        buffer = BytesIO()
        image.save(buffer, format="PNG")
        return buffer.getvalue()


class _PrimaryOnlyEmbyService(_FakeEmbyService):
    def upload_view_image(self, *, view_id: str, image_type: str, image_bytes: bytes, content_type: str = "image/png") -> None:
        if image_type == "Thumb":
            raise RuntimeError("thumb unsupported")
        super().upload_view_image(view_id=view_id, image_type=image_type, image_bytes=image_bytes, content_type=content_type)


class _ViewFallbackEmbyCoverService(EmbyCoverService):
    def __init__(self):
        super().__init__(base_url="https://emby.example", api_key="test-key")

    def _request_json(self, path: str):
        if path == "/UserViews":
            return {"Items": []}
        if path == "/Library/VirtualFolders":
            return [
                {
                    "ItemId": "vf-1",
                    "Name": "国产动漫",
                    "CollectionType": "tvshows",
                    "Type": "CollectionFolder",
                }
            ]
        return {"Items": []}


class _MergedViewEmbyCoverService(EmbyCoverService):
    def __init__(self):
        super().__init__(base_url="https://emby.example", api_key="test-key")

    def _request_json(self, path: str):
        if path == "/UserViews":
            return {
                "Items": [
                    {
                        "Id": "uv-1",
                        "Name": "国产动漫",
                        "CollectionType": "tvshows",
                        "Type": "CollectionFolder",
                        "RecursiveItemCount": 12,
                    }
                ]
            }
        if path == "/Library/VirtualFolders":
            return [
                {
                    "ItemId": "vf-1",
                    "Name": "国产动漫",
                    "CollectionType": "tvshows",
                    "Type": "CollectionFolder",
                    "ItemCount": 12,
                }
            ]
        return {"Items": []}


class _UploadProbeEmbyCoverService(EmbyCoverService):
    def __init__(self):
        super().__init__(base_url="https://emby.example", api_key="test-key")
        self.calls = []

    def _request_raw(self, path: str, *, method: str = "GET", body: bytes | None = None, headers: dict[str, str] | None = None) -> bytes:
        self.calls.append((method, path, body, headers))
        return b""


class _BackdropProbeEmbyCoverService(EmbyCoverService):
    def __init__(self):
        super().__init__(base_url="https://emby.example", api_key="test-key")
        self.paths = []

    def _request_bytes(self, path: str) -> bytes:
        self.paths.append(path)
        return b"backdrop-bytes"


class CoverStudioServiceTests(unittest.TestCase):
    def test_available_cover_fonts_reads_custom_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = pathlib.Path(temp_dir)
            (root / "aa_hui_yun_ti").mkdir(parents=True, exist_ok=True)
            (root / "aa_hui_yun_ti" / "AaHuiYunTi.ttf").write_bytes(b"fake-font")
            (root / "fonts.json").write_text(
                json.dumps(
                    [
                        {
                            "key": "aa_hui_yun_ti",
                            "label": "Aa绘云体",
                            "path": "aa_hui_yun_ti/AaHuiYunTi.ttf",
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            previous = os.environ.get("VISTAMIRROR_COVER_FONT_DIR")
            os.environ["VISTAMIRROR_COVER_FONT_DIR"] = temp_dir
            try:
                fonts = available_cover_fonts()
            finally:
                if previous is None:
                    os.environ.pop("VISTAMIRROR_COVER_FONT_DIR", None)
                else:
                    os.environ["VISTAMIRROR_COVER_FONT_DIR"] = previous

        self.assertTrue(any(row["key"] == "aa_hui_yun_ti" and row["label"] == "Aa绘云体" for row in fonts))

    def test_cover_studio_modes_include_new_showcase_templates(self) -> None:
        keys = {mode["key"] for mode in cover_studio_modes()}

        self.assertIn("fan_spread", keys)
        self.assertIn("banner_showcase", keys)
        self.assertIn("hero_showcase", keys)
        self.assertIn("gallery_wall_showcase", keys)
        self.assertIn("immersive_stage", keys)
        self.assertNotIn("stack_classic", keys)
        self.assertNotIn("rotated_stack", keys)
        self.assertNotIn("poster_wall", keys)
        self.assertNotIn("focus_poster", keys)
        self.assertNotIn("glory_view", keys)

    def test_normalize_cover_studio_config_clamps_sizes(self) -> None:
        config = normalize_cover_studio_config(
            {
                "draft": {
                    "templateKey": "banner_showcase",
                    "pickMode": "recent",
                    "titleFontSize": 999,
                    "subtitleFontSize": 1,
                    "overlayStrength": 1000,
                    "posterCount": 0,
                }
            }
        )
        self.assertEqual(config["draft"]["templateKey"], "banner_showcase")
        self.assertEqual(config["draft"]["pickMode"], "recent")
        self.assertEqual(config["draft"]["titleFontSize"], 180)
        self.assertEqual(config["draft"]["subtitleFontSize"], 22)
        self.assertEqual(config["draft"]["overlayStrength"], 0)
        self.assertEqual(config["draft"]["posterCount"], 2)

    def test_normalize_cover_studio_config_keeps_multiple_view_ids(self) -> None:
        config = normalize_cover_studio_config(
            {"draft": {"viewIds": ["view-1", "view-2", "view-1", " "]}}
        )

        self.assertEqual(config["draft"]["viewIds"], ["view-1", "view-2"])
        self.assertEqual(config["draft"]["viewId"], "view-1")

    def test_cover_studio_schedule_accepts_five_minute_cron(self) -> None:
        config = normalize_cover_studio_config(
            {"schedule": {"enabled": True, "cron": "*/5 * * * *"}}
        )

        self.assertTrue(config["schedule"]["enabled"])
        self.assertEqual(config["schedule"]["cron"], "*/5 * * * *")
        self.assertTrue(is_valid_cover_studio_cron("*/5 * * * *"))
        self.assertTrue(cron_matches(datetime(2026, 7, 11, 12, 10), "*/5 * * * *"))
        self.assertFalse(cron_matches(datetime(2026, 7, 11, 12, 11), "*/5 * * * *"))

    def test_cover_studio_schedule_rejects_invalid_cron(self) -> None:
        config = normalize_cover_studio_config(
            {"schedule": {"enabled": True, "cron": "every five minutes"}}
        )

        self.assertEqual(config["schedule"]["cron"], "0 */6 * * *")

    def test_cover_studio_schedule_is_independent_per_media_library(self) -> None:
        config = normalize_cover_studio_config(
            {
                "schedules": [
                    {
                        "id": "anime-plan",
                        "viewId": "view-1",
                        "viewName": "国产动漫",
                        "enabled": True,
                        "cron": "*/5 * * * *",
                        "template": {"templateKey": "fan_spread", "pickMode": "recent"},
                        "fingerprint": {"itemCount": 8, "latestItemId": "old", "latestCreatedAt": "2026-07-01"},
                    }
                ]
            }
        )
        self.assertEqual(len(config["schedules"]), 1)
        self.assertEqual(config["schedules"][0]["viewId"], "view-1")
        self.assertEqual(config["schedules"][0]["template"]["pickMode"], "recent")

        store = {"coverStudioConfig": config, "embyConfig": {"serverUrl": "https://emby.example", "apiKey": "key"}}

        class _SchedulerEmby:
            fingerprint = {"itemCount": 8, "latestItemId": "old", "latestCreatedAt": "2026-07-01"}

            def fetch_user_views(self):
                return [{"id": "view-1", "name": "国产动漫"}]

            def fetch_view_fingerprint(self, *, view_id):
                return dict(self.fingerprint)

            def fetch_view_items(self, *, view_id, pick_mode):
                return [{"id": "item-1"}]

        class _SchedulerCover:
            def __init__(self):
                self.generated = 0
                self.applied = 0

            def generate_preview(self, **kwargs):
                self.generated += 1
                return type("Preview", (), {"token": "preview"})()

            def backup_and_apply(self, *, config, **kwargs):
                self.applied += 1
                config["backups"] = {"view-1": {"primary": {"path": "backup"}}}

        emby = _SchedulerEmby()
        cover = _SchedulerCover()
        scheduler = CoverStudioScheduler(
            stop_event=threading.Event(),
            store_lock=threading.Lock(),
            read_store=lambda: store,
            write_store=lambda payload: store.update(payload),
            normalize_config=normalize_cover_studio_config,
            apply_emby_config=lambda value: value,
            build_emby_service=lambda value: emby,
            cover_service=cover,
            event_logger=lambda **kwargs: None,
        )

        unchanged = scheduler.run_once(plan_id="anime-plan")
        self.assertEqual(unchanged["results"][0]["status"], "no_change")
        self.assertEqual(cover.generated, 0)

        emby.fingerprint = {"itemCount": 9, "latestItemId": "new", "latestCreatedAt": "2026-07-02"}
        changed = scheduler.run_once(plan_id="anime-plan")
        self.assertTrue(changed["ok"])
        self.assertEqual(changed["results"][0]["status"], "success")
        self.assertEqual(cover.generated, 1)
        self.assertEqual(cover.applied, 1)

    def test_generate_preview_returns_data_url(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = CoverStudioService(data_dir=pathlib.Path(temp_dir))
            preview = service.generate_preview(
                view={"id": "view-1", "name": "国产动漫"},
                items=[
                    {"id": "1", "imageItemId": "1", "primaryTag": "a", "name": "A"},
                    {"id": "2", "imageItemId": "2", "primaryTag": "b", "name": "B"},
                    {"id": "3", "imageItemId": "3", "primaryTag": "c", "name": "C"},
                    {"id": "4", "imageItemId": "4", "primaryTag": "d", "name": "D"},
                ],
                template_key="fan_spread",
                font_key="hiragino",
                title_text="国产动漫",
                subtitle_text="Chinese Animation",
                title_font_size=108,
                subtitle_font_size=44,
                title_align="center",
                overlay_strength=58,
                poster_count=4,
                accent_tone="gold",
                poster_rotation=68,
                title_y_offset=-12,
                emby_service=_FakeEmbyService(),
            )
            self.assertTrue(preview.primary_image_path.exists())
            self.assertTrue(preview.primary_image_data_url.startswith("data:image/png;base64,"))
            self.assertEqual(preview.primary_width, 1600)
            self.assertEqual(preview.primary_height, 900)
            self.assertEqual(preview.template_key, "fan_spread")

    def test_generate_preview_supports_new_showcase_template(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = CoverStudioService(data_dir=pathlib.Path(temp_dir))
            preview = service.generate_preview(
                view={"id": "view-1", "name": "欧美电影"},
                items=[
                    {"id": "1", "imageItemId": "1", "primaryTag": "a", "name": "A"},
                    {"id": "2", "imageItemId": "2", "primaryTag": "b", "name": "B"},
                    {"id": "3", "imageItemId": "3", "primaryTag": "c", "name": "C"},
                    {"id": "4", "imageItemId": "4", "primaryTag": "d", "name": "D"},
                    {"id": "5", "imageItemId": "5", "primaryTag": "e", "name": "E"},
                ],
                template_key="banner_showcase",
                font_key="hiragino",
                title_text="欧美电影",
                subtitle_text="Western Movies",
                title_font_size=108,
                subtitle_font_size=44,
                title_align="left",
                overlay_strength=74,
                poster_count=5,
                accent_tone="gold",
                poster_rotation=18,
                title_y_offset=0,
                emby_service=_FakeEmbyService(),
            )
            self.assertTrue(preview.primary_image_path.exists())
            self.assertEqual(preview.template_key, "banner_showcase")

    def test_banner_showcase_variant_matches_p2_alignment_rules(self) -> None:
        cover_meta = get_cinematic_showcase_variant("banner", thumb=False)
        thumb_meta = get_cinematic_showcase_variant("banner", thumb=True)

        self.assertEqual(cover_meta["layout_style"], "streaming_banner")
        self.assertTrue(cover_meta["strict_poster_row"])
        self.assertGreaterEqual(cover_meta["frame_radius"], 40)
        self.assertNotIn("shelf_box", cover_meta)
        self.assertNotIn("shelf_alpha", cover_meta)
        self.assertEqual(cover_meta["angle_tune_scale"], 0.0)
        self.assertLessEqual(cover_meta["reflection_opacity"], 0.03)
        self.assertEqual({spec["rotation"] for spec in cover_meta["poster_specs"]}, {0.0})
        self.assertEqual({spec["size"] for spec in cover_meta["poster_specs"]}, {(246, 356)})
        self.assertEqual(
            {spec["origin"][1] + spec["size"][1] for spec in cover_meta["poster_specs"]},
            {814},
        )

        self.assertEqual(thumb_meta["layout_style"], "streaming_banner")
        self.assertTrue(thumb_meta["strict_poster_row"])
        self.assertGreaterEqual(thumb_meta["frame_radius"], 30)
        self.assertNotIn("shelf_box", thumb_meta)
        self.assertNotIn("shelf_alpha", thumb_meta)
        self.assertEqual(thumb_meta["angle_tune_scale"], 0.0)
        self.assertEqual({spec["rotation"] for spec in thumb_meta["poster_specs"]}, {0.0})
        self.assertEqual({spec["size"] for spec in thumb_meta["poster_specs"]}, {(196, 292)})
        self.assertEqual(
            {spec["origin"][1] + spec["size"][1] for spec in thumb_meta["poster_specs"]},
            {652},
        )

        for thumb in (False, True):
            for variant in ("hero", "gallery", "immersive"):
                meta = get_cinematic_showcase_variant(variant, thumb=thumb)
                for glass_key in (
                    "frame_radius",
                    "shelf_style",
                    "shelf_box",
                    "shelf_radius",
                    "shelf_alpha",
                    "shelf_blur",
                    "shelf_outline_alpha",
                    "shelf_highlight_alpha",
                ):
                    self.assertNotIn(glass_key, meta)

    def test_banner_showcase_prefers_emby_backdrop_for_hero(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = CoverStudioService(data_dir=pathlib.Path(temp_dir))
            emby = _BackdropEmbyService()
            preview = service.generate_preview(
                view={"id": "view-1", "name": "国产动漫"},
                items=[
                    {
                        "id": "series-1",
                        "imageItemId": "series-1",
                        "primaryTag": "poster-a",
                        "backdropImageItemId": "series-1",
                        "backdropTag": "backdrop-a",
                        "name": "A",
                    },
                    {"id": "2", "imageItemId": "2", "primaryTag": "b", "name": "B"},
                    {"id": "3", "imageItemId": "3", "primaryTag": "c", "name": "C"},
                    {"id": "4", "imageItemId": "4", "primaryTag": "d", "name": "D"},
                    {"id": "5", "imageItemId": "5", "primaryTag": "e", "name": "E"},
                ],
                template_key="banner_showcase",
                font_key="hiragino",
                title_text="国产动漫",
                subtitle_text="Chinese Animation",
                title_font_size=108,
                subtitle_font_size=44,
                title_align="left",
                overlay_strength=0,
                poster_count=5,
                accent_tone="gold",
                poster_rotation=100,
                title_y_offset=0,
                emby_service=emby,
            )
            rendered = Image.open(preview.primary_image_path).convert("RGB")
        self.assertEqual(emby.backdrop_requests, [("series-1", "backdrop-a", 1800)])
        self.assertGreater(rendered.getpixel((1300, 180))[0], 100)

    def test_backup_and_apply_keeps_only_primary_and_removes_thumb(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = CoverStudioService(data_dir=pathlib.Path(temp_dir))
            emby = _FakeEmbyService()
            preview = service.generate_preview(
                view={"id": "view-2", "name": "华语剧集"},
                items=[
                    {"id": "1", "imageItemId": "1", "primaryTag": "a", "name": "A"},
                    {"id": "2", "imageItemId": "2", "primaryTag": "b", "name": "B"},
                    {"id": "3", "imageItemId": "3", "primaryTag": "c", "name": "C"},
                    {"id": "4", "imageItemId": "4", "primaryTag": "d", "name": "D"},
                ],
                template_key="banner_showcase",
                font_key="hiragino",
                title_text="华语剧集",
                subtitle_text="Chinese Series",
                title_font_size=108,
                subtitle_font_size=44,
                title_align="left",
                overlay_strength=74,
                poster_count=5,
                accent_tone="gold",
                poster_rotation=18,
                title_y_offset=0,
                emby_service=emby,
            )
            config = default_cover_studio_config()
            result = service.backup_and_apply(
                config=config,
                view_id="view-2",
                preview_token=preview.token,
                emby_service=emby,
            )
        self.assertTrue(result["appliedAt"])
        self.assertEqual([row[1] for row in emby.uploads], ["Primary"])
        self.assertEqual(emby.deletes, [("view-2", "Thumb")])
        self.assertIn("view-2", config["backups"])

    def test_unknown_template_key_falls_back_to_fan_spread(self) -> None:
        config = normalize_cover_studio_config({"draft": {"templateKey": "unknown-mode"}})
        self.assertEqual(config["draft"]["templateKey"], "fan_spread")

    def test_fetch_user_views_falls_back_to_virtual_folders(self) -> None:
        service = _ViewFallbackEmbyCoverService()
        views = service.fetch_user_views()

        self.assertEqual(len(views), 1)
        self.assertEqual(views[0]["id"], "vf-1")
        self.assertEqual(views[0]["name"], "国产动漫")

    def test_fetch_user_views_merges_user_view_with_virtual_folder_target(self) -> None:
        service = _MergedViewEmbyCoverService()

        views = service.fetch_user_views()

        self.assertEqual(len(views), 1)
        self.assertEqual(views[0]["id"], "uv-1")
        self.assertEqual(views[0]["browseId"], "uv-1")
        self.assertEqual(views[0]["uploadTargetId"], "vf-1")
        self.assertEqual(views[0]["userViewId"], "uv-1")
        self.assertEqual(views[0]["virtualFolderId"], "vf-1")

    def test_upload_view_image_uses_emby_official_base64_post_shape(self) -> None:
        service = _UploadProbeEmbyCoverService()

        service.upload_view_image(view_id="54121", image_type="Primary", image_bytes=b"png-bytes", content_type="image/png")

        self.assertEqual(len(service.calls), 1)
        method, path, body, headers = service.calls[0]
        self.assertEqual(method, "POST")
        self.assertEqual(path, "/Items/54121/Images/Primary?Index=0")
        self.assertEqual(body, base64.b64encode(b"png-bytes"))
        self.assertEqual(headers["Content-Type"], "image/png")

    def test_fetch_backdrop_image_uses_emby_backdrop_endpoint(self) -> None:
        service = _BackdropProbeEmbyCoverService()

        payload = service.fetch_backdrop_image_bytes(item_id="series-1", image_tag="tag-a", max_width=1600)

        self.assertEqual(payload, b"backdrop-bytes")
        self.assertEqual(
            service.paths,
            ["/Items/series-1/Images/Backdrop?maxWidth=1600&quality=94&Index=0&tag=tag-a"],
        )

    def test_backup_and_apply_succeeds_when_thumb_upload_is_unsupported(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = CoverStudioService(data_dir=pathlib.Path(temp_dir))
            emby = _PrimaryOnlyEmbyService()
            preview = service.generate_preview(
                view={"id": "view-2", "name": "华语剧集"},
                items=[
                    {"id": "1", "imageItemId": "1", "primaryTag": "a", "name": "A"},
                    {"id": "2", "imageItemId": "2", "primaryTag": "b", "name": "B"},
                    {"id": "3", "imageItemId": "3", "primaryTag": "c", "name": "C"},
                    {"id": "4", "imageItemId": "4", "primaryTag": "d", "name": "D"},
                ],
                template_key="banner_showcase",
                font_key="hiragino",
                title_text="华语剧集",
                subtitle_text="Chinese Series",
                title_font_size=108,
                subtitle_font_size=44,
                title_align="left",
                overlay_strength=74,
                poster_count=5,
                accent_tone="gold",
                poster_rotation=18,
                title_y_offset=0,
                emby_service=emby,
            )
            config = default_cover_studio_config()
            result = service.backup_and_apply(
                config=config,
                view_id="view-2",
                preview_token=preview.token,
                emby_service=emby,
            )

        self.assertTrue(result["appliedAt"])
        self.assertEqual([row[1] for row in emby.uploads], ["Primary"])


if __name__ == "__main__":
    unittest.main()
