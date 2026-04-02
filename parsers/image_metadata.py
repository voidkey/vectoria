import io
import re
import logging
from dataclasses import dataclass
from PIL import Image

logger = logging.getLogger(__name__)

_IMG_REF_RE = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)

CONTEXT_CHARS = 200
MIN_DIMENSION = 100


@dataclass
class ImageMeta:
    filename: str
    index: int
    width: int | None
    height: int | None
    alt: str
    context: str
    section_title: str


_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def detect_mime_type(data: bytes, fallback: str = "application/octet-stream") -> str:
    """Detect image MIME type from magic bytes.

    Returns the detected MIME type, or *fallback* when the format is
    unrecognised (default ``application/octet-stream``).
    """
    if len(data) < 2:
        return fallback
    if data[:8] == _PNG_SIGNATURE:
        return "image/png"
    if data[:2] == b"\xff\xd8":
        return "image/jpeg"
    if data[:4] == b"GIF8":
        return "image/gif"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if len(data) >= 4 and data[:2] in (b"II", b"MM") and data[2:4] in (b"\x2a\x00", b"\x00\x2a"):
        return "image/tiff"
    if data[:2] == b"BM":
        return "image/bmp"
    return fallback


def _get_png_dimensions(img_bytes: bytes) -> tuple[int | None, int | None]:
    """Parse PNG IHDR chunk directly to extract width and height."""
    import struct
    # PNG signature is 8 bytes, then chunk: 4 len + 4 type + data + 4 crc
    # IHDR is always the first chunk; its data starts at offset 16
    if len(img_bytes) >= 24 and img_bytes[:8] == _PNG_SIGNATURE:
        try:
            width, height = struct.unpack(">II", img_bytes[16:24])
            return width, height
        except struct.error:
            pass
    return None, None


def _get_dimensions(img_bytes: bytes) -> tuple[int | None, int | None]:
    """Read image dimensions from bytes using PIL, with PNG fallback parser."""
    try:
        with Image.open(io.BytesIO(img_bytes)) as img:
            return img.size  # (width, height)
    except Exception:
        # PIL may fail on truncated images (e.g. PNG with no IDAT); try raw parse
        return _get_png_dimensions(img_bytes)


def _find_section_title(markdown: str, pos: int) -> str:
    """Find the nearest heading before the given position."""
    best_title = ""
    for m in _HEADING_RE.finditer(markdown):
        if m.start() > pos:
            break
        best_title = m.group(2).strip()
    return best_title


def _extract_context(markdown: str, start: int, end: int) -> str:
    """Extract ~CONTEXT_CHARS characters before and after the image reference."""
    before_start = max(0, start - CONTEXT_CHARS)
    after_end = min(len(markdown), end + CONTEXT_CHARS)
    before = markdown[before_start:start].strip()
    after = markdown[end:after_end].strip()
    parts = [p for p in (before, after) if p]
    return "\n\n".join(parts)


def extract_image_metadata(
    markdown: str, images: dict[str, bytes]
) -> list[ImageMeta]:
    """Extract metadata for each image found in both the markdown and image dict.

    Filters out images smaller than MIN_DIMENSION (100px) on either axis.
    Returns a list of ImageMeta sorted by document order.
    """
    refs: list[tuple[str, int, int, str]] = []  # (filename, start, end, alt)
    for m in _IMG_REF_RE.finditer(markdown):
        alt = m.group(1)
        ref_path = m.group(2)
        for img_name in images:
            if img_name in ref_path or ref_path in img_name or img_name == ref_path:
                refs.append((img_name, m.start(), m.end(), alt))
                break

    # Also include images not referenced in markdown
    referenced_names = {r[0] for r in refs}
    for img_name in images:
        if img_name not in referenced_names:
            refs.append((img_name, len(markdown), len(markdown), ""))

    result: list[ImageMeta] = []
    idx = 0
    for filename, start, end, alt in refs:
        img_bytes = images[filename]
        w, h = _get_dimensions(img_bytes)

        if w is not None and h is not None:
            if w < MIN_DIMENSION or h < MIN_DIMENSION:
                continue

        context = _extract_context(markdown, start, end)
        section_title = _find_section_title(markdown, start)

        result.append(ImageMeta(
            filename=filename,
            index=idx,
            width=w,
            height=h,
            alt=alt,
            context=context,
            section_title=section_title,
        ))
        idx += 1

    return result
