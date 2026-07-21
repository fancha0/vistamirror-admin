from __future__ import annotations

from typing import Any


# Cover Studio only creates Emby Primary artwork.  The layouts below therefore
# use the full-size Primary canvas and never maintain a parallel Thumb variant.
CINEMATIC_SHOWCASE_VARIANTS: dict[str, dict[str, Any]] = {
    "banner": {
        "layout_style": "streaming_banner",
        "frame_inset": 38,
        "frame_radius": 44,
        "frame_outline_alpha": 116,
        "left_panel_ratio": 0.36,
        "glow_alpha": 28,
        "floor_glow_alpha": 0,
        "hero_box": (38, 38, 1524, 824),
        "hero_radius": 44,
        "hero_mask_blur": 0,
        "hero_opacity": 1.0,
        "hero_ring_alpha": 0,
        "poster_limit": 5,
        "reflection_opacity": 0.028,
        "reflection_scale": 0.22,
        "angle_tune_scale": 0.0,
        "strict_poster_row": True,
        "title": {
            "x_bounds": (116, 530),
            "base_y": 136,
            "decor": {"marker": "vertical", "marker_gap": 32, "marker_width": 6, "subtitle_gap": 18},
        },
        "poster_specs": [
            {"origin": (64, 458), "size": (246, 356), "rotation": 0.0, "radius": 28, "elevation": 1, "glow_alpha": 24},
            {"origin": (330, 458), "size": (246, 356), "rotation": 0.0, "radius": 28, "elevation": 1, "glow_alpha": 24},
            {"origin": (596, 458), "size": (246, 356), "rotation": 0.0, "radius": 28, "elevation": 1, "glow_alpha": 24},
            {"origin": (862, 458), "size": (246, 356), "rotation": 0.0, "radius": 28, "elevation": 1, "glow_alpha": 24},
            {"origin": (1128, 458), "size": (246, 356), "rotation": 0.0, "radius": 28, "elevation": 1, "glow_alpha": 24},
        ],
    },
    "hero": {
        "left_panel_ratio": 0.4,
        "glow_alpha": 70,
        "floor_glow_alpha": 26,
        "hero_box": (760, 18, 782, 690),
        "hero_radius": 62,
        "hero_mask_blur": 28,
        "hero_opacity": 0.66,
        "hero_ring_alpha": 42,
        "poster_limit": 5,
        "reflection_opacity": 0.12,
        "reflection_scale": 0.54,
        "title": {
            "x_bounds": (86, 618),
            "base_y": 88,
            "decor": {"line_gap": 18, "line_length": 96, "line_height": 6, "subtitle_gap": 18},
        },
        "poster_specs": [
            {"origin": (92, 562), "size": (206, 292), "rotation": -4.2, "radius": 24, "elevation": 1},
            {"origin": (326, 532), "size": (218, 310), "rotation": -2.0, "radius": 24, "elevation": 2},
            {"origin": (592, 488), "size": (262, 372), "rotation": 0.0, "radius": 28, "elevation": 6, "glow_alpha": 136},
            {"origin": (896, 532), "size": (218, 310), "rotation": 1.8, "radius": 24, "elevation": 2},
            {"origin": (1140, 562), "size": (206, 292), "rotation": 4.1, "radius": 24, "elevation": 1},
        ],
    },
    "gallery": {
        "left_panel_ratio": 0.38,
        "glow_alpha": 52,
        "floor_glow_alpha": 20,
        "hero_box": (860, 42, 640, 490),
        "hero_radius": 48,
        "hero_mask_blur": 24,
        "hero_opacity": 0.42,
        "hero_ring_alpha": 24,
        "poster_limit": 6,
        "reflection_opacity": 0.1,
        "reflection_scale": 0.48,
        "title": {
            "x_bounds": (84, 612),
            "base_y": 86,
            "decor": {"line_gap": 16, "line_length": 88, "line_height": 6, "subtitle_gap": 16},
        },
        "poster_specs": [
            {"origin": (86, 566), "size": (184, 266), "rotation": -2.2, "radius": 22, "elevation": 1},
            {"origin": (292, 552), "size": (192, 276), "rotation": -1.4, "radius": 22, "elevation": 2},
            {"origin": (506, 536), "size": (198, 284), "rotation": -0.6, "radius": 22, "elevation": 3},
            {"origin": (722, 536), "size": (198, 284), "rotation": 0.6, "radius": 22, "elevation": 3},
            {"origin": (938, 552), "size": (192, 276), "rotation": 1.4, "radius": 22, "elevation": 2},
            {"origin": (1144, 566), "size": (184, 266), "rotation": 2.2, "radius": 22, "elevation": 1},
        ],
    },
    "immersive": {
        "left_panel_ratio": 0.43,
        "glow_alpha": 74,
        "floor_glow_alpha": 34,
        "hero_box": (732, 34, 808, 624),
        "hero_radius": 56,
        "hero_mask_blur": 26,
        "hero_opacity": 0.58,
        "hero_ring_alpha": 46,
        "poster_limit": 5,
        "reflection_opacity": 0.15,
        "reflection_scale": 0.58,
        "title": {
            "x_bounds": (92, 650),
            "base_y": 104,
            "decor": {"line_gap": 20, "line_length": 96, "line_height": 6, "subtitle_gap": 18},
        },
        "poster_specs": [
            {"origin": (108, 564), "size": (204, 288), "rotation": -3.2, "radius": 24, "elevation": 1},
            {"origin": (344, 538), "size": (220, 312), "rotation": -1.6, "radius": 24, "elevation": 2},
            {"origin": (620, 500), "size": (248, 352), "rotation": 0.0, "radius": 28, "elevation": 6, "glow_alpha": 138},
            {"origin": (914, 536), "size": (220, 312), "rotation": 1.8, "radius": 24, "elevation": 2},
            {"origin": (1162, 564), "size": (204, 288), "rotation": 3.2, "radius": 24, "elevation": 1},
        ],
    },
}


# These layouts are Primary-only compositions.  They intentionally describe the
# scene language rather than hard-code a media-library title or artwork so the
# existing cover studio inputs remain the single source of content.
PRIMARY_LAYOUT_VARIANTS: dict[str, dict[str, Any]] = {
    "bookshelf": {
        "layout_style": "bookshelf",
        "poster_limit": 7,
        "title": {
            "x_bounds": (72, 680),
            "base_y": 70,
            "decor": {
                "line_gap": 16,
                "line_length": 74,
                "line_height": 4,
                "subtitle_gap": 16,
                "title_fill": (248, 241, 226, 255),
                "subtitle_fill": (209, 190, 161, 228),
                "line_fill": (218, 158, 82, 224),
                "soft_shadow": True,
            },
        },
    },
    "bookshelf_two": {
        "layout_style": "bookshelf_two",
        "poster_limit": 7,
        "title": {
            "x_bounds": (78, 560),
            "base_y": 350,
            "decor": {
                "line_gap": 18,
                "line_length": 84,
                "line_height": 4,
                "subtitle_gap": 17,
                "title_fill": (246, 242, 232, 255),
                "subtitle_fill": (207, 191, 169, 226),
                "line_fill": (213, 166, 98, 220),
                "soft_shadow": True,
            },
        },
    },
    "honeycomb": {
        "layout_style": "honeycomb",
        "poster_limit": 7,
        "title": {
            "x_bounds": (72, 620),
            "base_y": 310,
            "decor": {"line_gap": 16, "line_length": 84, "line_height": 5, "subtitle_gap": 16},
        },
    },
    "panorama": {
        "layout_style": "panorama_gallery",
        "poster_limit": 7,
        "title": {
            "x_bounds": (102, 470),
            "base_y": 370,
            "decor": {
                "line_gap": 18,
                "line_length": 72,
                "line_height": 4,
                "subtitle_gap": 17,
                "title_fill": (246, 244, 237, 255),
                "subtitle_fill": (181, 193, 208, 218),
                "line_fill": (205, 159, 92, 215),
                "soft_shadow": True,
            },
        },
    },
}


def get_cinematic_showcase_variant(variant: str) -> dict[str, Any]:
    safe_variant = str(variant or "banner").strip().lower() or "banner"
    return CINEMATIC_SHOWCASE_VARIANTS.get(safe_variant, CINEMATIC_SHOWCASE_VARIANTS["banner"])


def get_primary_layout_variant(variant: str) -> dict[str, Any]:
    safe_variant = str(variant or "bookshelf").strip().lower() or "bookshelf"
    return PRIMARY_LAYOUT_VARIANTS.get(safe_variant, PRIMARY_LAYOUT_VARIANTS["bookshelf"])
