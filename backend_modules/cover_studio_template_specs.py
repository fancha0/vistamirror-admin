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
            "x_bounds": (72, 700),
            "base_y": 72,
            "decor": {"line_gap": 16, "line_length": 88, "line_height": 5, "subtitle_gap": 16},
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
            "x_bounds": (74, 670),
            "base_y": 74,
            "decor": {"line_gap": 16, "line_length": 88, "line_height": 5, "subtitle_gap": 16},
        },
    },
}


# The fan layout is a mirrored poster fan, not a wave or generic card row.
# It is rendered on the Emby Primary canvas only.
FAN_SPREAD_LAYOUT: dict[str, Any] = {
    "title_area_ratio": 0.32,
    "poster_limit": 7,
    # The UI rotation control scales these fixed, mirrored fan angles instead
    # of introducing a shared tilt that would break the two fan halves.
    "rotation_tune_scale": 0.004,
    "reflection_opacity": 0.065,
    "reflection_scale": 0.20,
    "horizontal_inset": 54,
    "poster_aspect_ratio": 0.633,
    "min_poster_height": 350,
    "max_poster_height": 590,
    "poster_overlap_ratio": 0.10,
    "focus_scale": 1.10,
    "fan_drop_ratio": 0.095,
    "shelf_box": (132, 594, 1548, 838),
    "shelf_radius": 52,
    "shelf_alpha": 38,
    "shelf_border_alpha": 0,
    "title": {
        "x_bounds": (78, 486),
        "base_y": 108,
    },
    "poster_specs": [
        # Seven cards orbit a shared lower pivot. Their angles are fixed mirror
        # pairs so the visual reads as one fan with a clear center hinge.
        {"rotation": -12.0, "elevation": 1},
        {"rotation": -8.0, "elevation": 2},
        {"rotation": -4.0, "elevation": 3},
        {"rotation": 0.0, "elevation": 8},
        {"rotation": 4.0, "elevation": 3},
        {"rotation": 8.0, "elevation": 2},
        {"rotation": 12.0, "elevation": 1},
    ],
}


def get_fan_spread_layout() -> dict[str, Any]:
    return FAN_SPREAD_LAYOUT


def get_cinematic_showcase_variant(variant: str) -> dict[str, Any]:
    safe_variant = str(variant or "banner").strip().lower() or "banner"
    return CINEMATIC_SHOWCASE_VARIANTS.get(safe_variant, CINEMATIC_SHOWCASE_VARIANTS["banner"])


def get_primary_layout_variant(variant: str) -> dict[str, Any]:
    safe_variant = str(variant or "bookshelf").strip().lower() or "bookshelf"
    return PRIMARY_LAYOUT_VARIANTS.get(safe_variant, PRIMARY_LAYOUT_VARIANTS["bookshelf"])
