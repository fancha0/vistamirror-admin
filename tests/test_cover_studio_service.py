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
from backend_modules.cover_studio_template_specs import (
    get_cinematic_showcase_variant,
    get_primary_layout_variant,
)


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

    def test_cover_font_selection_excludes_removed_fonts_and_migrates_old_configs(self) -> None:
        removed_keys = {"hiragino", "noteworthy", "avenir", "fu_lu_da_mao_bi_ti", "the_mordeus"}
        self.assertFalse(removed_keys.intersection({row["key"] for row in available_cover_fonts()}))

        config = normalize_cover_studio_config(
            {
                "draft": {"fontKey": "hiragino"},
                "presets": [{"id": "legacy", "fontKey": "avenir"}],
                "schedules": [
                    {
                        "id": "legacy-plan",
                        "viewId": "view-1",
                        "template": {"fontKey": "the_mordeus"},
                    }
                ],
            }
        )

        self.assertEqual(config["draft"]["fontKey"], "heiti")
        self.assertEqual(config["presets"][0]["fontKey"], "heiti")
        self.assertEqual(config["schedules"][0]["template"]["fontKey"], "heiti")

    def test_cover_studio_modes_include_new_showcase_templates(self) -> None:
        keys = {mode["key"] for mode in cover_studio_modes()}

        self.assertNotIn("fan_spread", keys)
        self.assertNotIn("fan_poster_cover", keys)
        self.assertIn("banner_showcase", keys)
        self.assertIn("hero_showcase", keys)
        self.assertIn("gallery_wall_showcase", keys)
        self.assertIn("immersive_stage", keys)
        self.assertIn("bookshelf_gallery", keys)
        self.assertIn("bookshelf_gallery_2", keys)
        self.assertIn("honeycomb_hex", keys)
        self.assertIn("panorama_gallery", keys)
        self.assertNotIn("stack_classic", keys)
        self.assertNotIn("rotated_stack", keys)
        self.assertNotIn("poster_wall", keys)
        self.assertNotIn("focus_poster", keys)
        self.assertNotIn("glory_view", keys)

    def test_primary_layout_templates_are_primary_only_and_renderable(self) -> None:
        expected_variants = {
            "bookshelf_gallery": "bookshelf",
            "bookshelf_gallery_2": "bookshelf_two",
            "honeycomb_hex": "honeycomb",
            "panorama_gallery": "panorama",
        }
        modes = {mode["key"]: mode for mode in cover_studio_modes()}
        images = [Image.new("RGB", (420, 630), (36 + index * 20, 74, 142)) for index in range(7)]

        for template_key, variant in expected_variants.items():
            with self.subTest(template_key=template_key):
                self.assertEqual(modes[template_key]["family"], "primary_layout")
                self.assertEqual(modes[template_key]["variant"], variant)
                self.assertEqual(modes[template_key]["maxPosterCount"], 7)
                self.assertNotIn("posterRotation", modes[template_key]["supports"])
                self.assertEqual(get_primary_layout_variant(variant)["poster_limit"], 7)
                canvas = CoverStudioService(data_dir=pathlib.Path(tempfile.gettempdir()))._render_mode_cover(
                    template_key=template_key,
                    images=images,
                    hero_image=None,
                    title_text="影视精选",
                    subtitle_text="Media Collection",
                    font_key="heiti",
                    title_font_size=108,
                    subtitle_font_size=44,
                    title_align="left",
                    overlay_strength=0,
                    accent_tone="gold",
                    poster_rotation=0,
                    title_y_offset=0,
                )
                self.assertEqual(canvas.size, (1600, 900))

    def test_panoramic_gallery_slots_are_mirrored_and_keep_a_center_focus(self) -> None:
        slots = CoverStudioService._panoramic_gallery_slots(count=7)

        self.assertEqual([slot["rotation"] for slot in slots], [0.0] * 7)
        self.assertEqual([slot["scale"] for slot in slots], [0.88, 0.92, 0.98, 1.18, 0.98, 0.92, 0.88])
        self.assertTrue(slots[3]["is_focus"])
        self.assertEqual(len(CoverStudioService._panoramic_gallery_slots(count=3)), 3)

    def test_bookshelf_primary_layout_uses_walnut_collection_scene(self) -> None:
        service = CoverStudioService(data_dir=pathlib.Path(tempfile.gettempdir()))
        images = [Image.new("RGB", (420, 630), (36 + index * 20, 74, 142)) for index in range(7)]

        canvas = service._render_primary_layout_cover(
            images=images,
            tone={"accent": (202, 145, 74), "glow": (90, 138, 194), "soft": (210, 190, 160)},
            variant="bookshelf",
        )

        # The bottom shelf is opaque walnut wood, not the old perforated rail.
        self.assertGreater(canvas.getpixel((800, 772))[0], canvas.getpixel((800, 772))[2])
        self.assertGreater(canvas.getpixel((800, 772))[3], 200)
        self.assertEqual(service._bookshelf_poster_rotations(count=7), [0.0] * 7)

    def test_bookshelf_two_splits_seven_posters_across_two_walnut_shelves(self) -> None:
        slots = CoverStudioService._bookshelf_two_slots(top_count=4, bottom_count=3, canvas_size=(1600, 900))

        self.assertEqual(len(slots), 7)
        self.assertEqual(len({slot["origin"][1] for slot in slots[:4]}), 2)
        self.assertTrue(all(slot["origin"][1] < 500 for slot in slots[:4]))
        self.assertTrue(all(slot["origin"][1] > 500 for slot in slots[4:]))

    def test_banner_showcase_exposes_its_fixed_layout_constraints(self) -> None:
        banner = next(mode for mode in cover_studio_modes() if mode["key"] == "banner_showcase")
        config = normalize_cover_studio_config(
            {
                "draft": {
                    "templateKey": "banner_showcase",
                    "posterCount": 8,
                    "posterRotation": 76,
                }
            }
        )

        self.assertEqual(banner["maxPosterCount"], 5)
        self.assertNotIn("posterRotation", banner["supports"])
        self.assertEqual(config["draft"]["posterCount"], 5)
        self.assertEqual(config["draft"]["posterRotation"], 0)

    def test_cover_poster_source_falls_back_from_primary_to_thumb(self) -> None:
        class _ThumbFallbackEmby:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def fetch_primary_image_bytes(self, **_kwargs) -> bytes:
                self.calls.append("primary")
                raise RuntimeError("primary unavailable")

            def fetch_item_image_bytes(self, **_kwargs) -> bytes:
                self.calls.append("thumb")
                image = Image.new("RGB", (420, 630), (42, 96, 184))
                buffer = BytesIO()
                image.save(buffer, format="PNG")
                return buffer.getvalue()

        emby = _ThumbFallbackEmby()
        image = CoverStudioService._load_item_poster_image(
            emby_service=emby,
            item={"id": "item-1", "imageItemId": "item-1", "thumbImageItemId": "item-1"},
        )

        self.assertIsNotNone(image)
        self.assertEqual(emby.calls, ["primary", "thumb"])

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

    def test_normalize_cover_studio_config_corrects_legacy_western_movies_subtitle(self) -> None:
        config = normalize_cover_studio_config(
            {
                "draft": {"subtitleText": "Western Movies"},
                "schedules": [
                    {
                        "id": "western-library",
                        "viewId": "view-western",
                        "template": {"subtitleText": "Western Movies"},
                    }
                ],
            }
        )

        self.assertEqual(config["draft"]["subtitleText"], "Western Cinema")
        self.assertEqual(config["schedules"][0]["template"]["subtitleText"], "Western Cinema")

    def test_cover_studio_schedule_accepts_five_minute_cron(self) -> None:
        config = normalize_cover_studio_config(
            {"schedule": {"enabled": True, "cron": "*/5 * * * *"}}
        )

        self.assertTrue(config["schedule"]["enabled"])
        self.assertEqual(config["schedule"]["cron"], "*/5 * * * *")
        self.assertTrue(is_valid_cover_studio_cron("*/5 * * * *"))
        self.assertTrue(cron_matches(datetime(2026, 7, 11, 12, 10), "*/5 * * * *"))
        self.assertFalse(cron_matches(datetime(2026, 7, 11, 12, 11), "*/5 * * * *"))

    def test_cover_studio_schedule_draft_survives_config_reload(self) -> None:
        config = normalize_cover_studio_config(
            {
                "scheduleDraft": {
                    "templateKey": "hero_showcase",
                    "pickMode": "recent",
                    "titleText": "欧美剧集",
                    "subtitleText": "Western Series",
                    "fontKey": "gong_fan_nu_fang_ti",
                    "titleFontSize": 132,
                    "subtitleFontSize": 48,
                    "titleAlign": "center",
                    "posterCount": 4,
                    "accentTone": "neutral",
                    "posterRotation": 18,
                    "titleYOffset": -24,
                }
            }
        )
        reloaded = normalize_cover_studio_config(config)

        self.assertEqual(reloaded["scheduleDraft"]["templateKey"], "hero_showcase")
        self.assertEqual(reloaded["scheduleDraft"]["pickMode"], "recent")
        self.assertEqual(reloaded["scheduleDraft"]["fontKey"], "gong_fan_nu_fang_ti")
        self.assertEqual(reloaded["scheduleDraft"]["posterCount"], 4)
        self.assertEqual(reloaded["scheduleDraft"]["posterRotation"], 18)
        self.assertEqual(reloaded["scheduleDraft"]["titleYOffset"], -24)

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
                        "template": {
                            "templateKey": "banner_showcase",
                            "pickMode": "recent",
                            "posterCount": 7,
                            "posterRotation": 0,
                            "titleYOffset": -48,
                        },
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
                self.preview_kwargs = {}

            def generate_preview(self, **kwargs):
                self.generated += 1
                self.preview_kwargs = dict(kwargs)
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
        self.assertEqual(cover.preview_kwargs["poster_count"], 5)
        self.assertEqual(cover.preview_kwargs["poster_rotation"], 0)
        self.assertEqual(cover.preview_kwargs["title_y_offset"], -48)

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
                template_key="banner_showcase",
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
        self.assertEqual(preview.template_key, "banner_showcase")
        self.assertEqual(len(preview.selected_items), 4)

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
                subtitle_text="Western Cinema",
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
        cover_meta = get_cinematic_showcase_variant("banner")

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

        for variant in ("hero", "gallery", "immersive"):
            meta = get_cinematic_showcase_variant(variant)
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

    def test_backup_and_apply_writes_primary_only(self) -> None:
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
        self.assertEqual(emby.deletes, [])
        self.assertIn("view-2", config["backups"])

    def test_removed_and_unknown_template_keys_fall_back_to_banner_showcase(self) -> None:
        config = normalize_cover_studio_config({"draft": {"templateKey": "unknown-mode"}})
        self.assertEqual(config["draft"]["templateKey"], "banner_showcase")
        migrated = normalize_cover_studio_config({"draft": {"templateKey": "fan_spread"}})
        self.assertEqual(migrated["draft"]["templateKey"], "banner_showcase")
        migrated = normalize_cover_studio_config({"draft": {"templateKey": "fan_poster_cover"}})
        self.assertEqual(migrated["draft"]["templateKey"], "banner_showcase")

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

if __name__ == "__main__":
    unittest.main()
