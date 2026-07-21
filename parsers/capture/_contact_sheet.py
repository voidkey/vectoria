"""Pure-Pillow contact sheets — stitch labeled image grids into JPEG pages.

Port of hyperframes' contactSheet.ts (createContactSheet / createContactSheetPages),
implemented with Pillow instead of sharp+SVG-overlay so it is UNIT-TESTABLE without a
browser: bytes in, JPEG page bytes out. A contact sheet packs many screenshots/assets
into one numbered grid with a label under each cell, which is far cheaper for an agent
to read than the images individually.

The SVG/lottie *rasterization* that produces some of the input bytes is the
browser-dependent part; it lives in the orchestrator (best-effort). This module only
does the pure layout + JPEG encode.
"""
from __future__ import annotations

import io
import logging

from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

# Reference palette (contactSheet.ts): near-black canvas, white bold labels.
_BG = (26, 26, 26)
_LABEL_BG = (26, 26, 26)
_LABEL_FG = (255, 255, 255)
_PADDING = 4
_LABEL_H = 26
_JPEG_QUALITY = 88


def _font() -> ImageFont.ImageFont:
    """A 13px bold-ish font. Falls back to Pillow's bundled bitmap font when no
    system TrueType face is available (keeps this dependency-free + deterministic)."""
    for name in ("DejaVuSans-Bold.ttf", "Arial Bold.ttf", "Helvetica.ttf"):
        try:
            return ImageFont.truetype(name, 13)
        except Exception:
            continue
    return ImageFont.load_default()


def _load(data: bytes) -> Image.Image | None:
    try:
        im = Image.open(io.BytesIO(data))
        im.load()
        return im.convert("RGB")
    except Exception:
        return None


def _contain(im: Image.Image, cell_w: int, cell_h: int) -> Image.Image:
    """Aspect-preserving contain onto a cell_w × cell_h canvas (no cropping),
    padded with the background color. Mirrors sharp's fit:"contain"."""
    canvas = Image.new("RGB", (cell_w, cell_h), _BG)
    sw, sh = im.size
    if sw <= 0 or sh <= 0:
        return canvas
    scale = min(cell_w / sw, cell_h / sh)
    nw, nh = max(1, int(sw * scale)), max(1, int(sh * scale))
    resized = im.resize((nw, nh), Image.LANCZOS)
    canvas.paste(resized, ((cell_w - nw) // 2, (cell_h - nh) // 2))
    return canvas


def _truncate(label: str) -> str:
    return (label[:57] + "...") if len(label) > 60 else label


def _build_page(items: list[tuple[bytes, str]], cols: int, thumb_w: int,
                font) -> bytes | None:
    """Render ONE page (already ≤ per_page items) to JPEG bytes. Cell height comes
    from the FIRST decodable image's aspect ratio (reference parity). Returns None
    when no item decodes."""
    decoded: list[tuple[Image.Image, str]] = []
    for data, label in items:
        im = _load(data)
        if im is not None:
            decoded.append((im, label))
    if not decoded:
        return None

    first = decoded[0][0]
    sw, sh = first.size
    cell_w = thumb_w
    cell_h = max(1, round(sh * (thumb_w / sw))) if sw else thumb_w

    rows = (len(decoded) + cols - 1) // cols
    total_w = cols * cell_w + (cols + 1) * _PADDING
    total_h = rows * (cell_h + _LABEL_H) + (rows + 1) * _PADDING
    sheet = Image.new("RGB", (total_w, total_h), _BG)
    draw = ImageDraw.Draw(sheet)

    for i, (im, label) in enumerate(decoded):
        col = i % cols
        row = i // cols
        x = _PADDING + col * (cell_w + _PADDING)
        y = _PADDING + row * (cell_h + _LABEL_H + _PADDING)
        # Label strip (index + text), then the contained image below it.
        draw.rectangle([x, y, x + cell_w, y + _LABEL_H], fill=_LABEL_BG)
        text = _truncate(f"{i + 1}. {label}" if label else f"{i + 1}")
        draw.text((x + 8, y + 6), text, fill=_LABEL_FG, font=font)
        sheet.paste(_contain(im, cell_w, cell_h), (x, y + _LABEL_H))

    buf = io.BytesIO()
    sheet.save(buf, format="JPEG", quality=_JPEG_QUALITY)
    return buf.getvalue()


def build_contact_sheet(items: list[tuple[bytes, str]], *, cols: int,
                        per_page: int, thumb_w: int) -> list[bytes]:
    """Paginate ``items`` into contact-sheet JPEG pages.

    ``items`` is a list of (image_bytes, label). Splits into pages of ``per_page``,
    lays each out on a ``cols``-wide grid (aspect-preserving contain, label under each
    cell), and returns one JPEG's bytes per page. Empty input -> ``[]``. A page whose
    images all fail to decode is skipped (logged). Pure — no I/O, no browser."""
    if not items:
        return []
    font = _font()
    pages: list[bytes] = []
    for start in range(0, len(items), per_page):
        chunk = items[start:start + per_page]
        try:
            page = _build_page(chunk, cols, thumb_w, font)
        except Exception:
            logger.info("contact sheet: page build failed", exc_info=True)
            continue
        if page is not None:
            pages.append(page)
    return pages
