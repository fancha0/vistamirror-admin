from __future__ import annotations

from pathlib import Path
from typing import Iterable

from PIL import Image, ImageDraw, ImageFilter


ROOT = Path(__file__).resolve().parent


POINTS = [
    (58, 70),
    (120, 190),
    (165, 118),
    (196, 146),
    (196, 94),
]


def interpolate_color(t: float) -> tuple[int, int, int, int]:
    stops = [
        (0.0, (122, 87, 255, 255)),
        (0.45, (20, 143, 255, 255)),
        (1.0, (34, 231, 244, 255)),
    ]
    for index in range(len(stops) - 1):
        start_offset, start_color = stops[index]
        end_offset, end_color = stops[index + 1]
        if start_offset <= t <= end_offset:
            local = (t - start_offset) / (end_offset - start_offset)
            return tuple(
                round(start + (end - start) * local)
                for start, end in zip(start_color, end_color)
            )
    return stops[-1][1]


def scale_points(size: int) -> list[tuple[float, float]]:
    scale = size / 256
    return [(x * scale, y * scale) for x, y in POINTS]


def iter_segments(points: Iterable[tuple[float, float]]) -> list[tuple[tuple[float, float], tuple[float, float]]]:
    pts = list(points)
    return list(zip(pts[:-1], pts[1:]))


def segment_length(start: tuple[float, float], end: tuple[float, float]) -> float:
    return ((end[0] - start[0]) ** 2 + (end[1] - start[1]) ** 2) ** 0.5


def draw_gradient_line(size: int) -> Image.Image:
    base = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    glow = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    base_draw = ImageDraw.Draw(base)
    glow_draw = ImageDraw.Draw(glow)
    points = scale_points(size)
    total_length = sum(segment_length(start, end) for start, end in iter_segments(points)) or 1
    traversed = 0.0
    stroke = max(8, round(size * 0.11))

    for start, end in iter_segments(points):
        length = segment_length(start, end)
        steps = max(2, round(length / max(2, size * 0.008)))
        for step in range(steps + 1):
            t = step / steps
            x = start[0] + (end[0] - start[0]) * t
            y = start[1] + (end[1] - start[1]) * t
            global_t = min(1.0, (traversed + length * t) / total_length)
            color = interpolate_color(global_t)
            base_draw.ellipse(
                (x - stroke / 2, y - stroke / 2, x + stroke / 2, y + stroke / 2),
                fill=color,
            )
            glow_color = color[:3] + (120,)
            glow_draw.ellipse(
                (
                    x - stroke * 0.62,
                    y - stroke * 0.62,
                    x + stroke * 0.62,
                    y + stroke * 0.62,
                ),
                fill=glow_color,
            )
        traversed += length

    glow = glow.filter(ImageFilter.GaussianBlur(radius=max(1, size * 0.018)))
    output = Image.alpha_composite(glow, base)
    return output


def export(size: int, filename: str) -> None:
    image = draw_gradient_line(size)
    image.save(ROOT / filename, format="PNG")


def main() -> None:
    export(512, "android-chrome-512x512.png")
    export(192, "android-chrome-192x192.png")
    export(180, "apple-touch-icon.png")
    export(1024, "logo-fallback-1024.png")


if __name__ == "__main__":
    main()
