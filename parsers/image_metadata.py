import io
import re
import logging
from PIL import Image

from parsers.image_ref import ImageRef

logger = logging.getLogger(__name__)

_IMG_REF_RE = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)

CONTEXT_CHARS = 200
MIN_DIMENSION = 100


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


def extract_metadata_into_refs(
    markdown: str, refs: list[ImageRef],
) -> list[ImageRef]:
    """Fill ``alt``, ``context``, ``section_title``, ``markdown_pos`` on
    each ref by matching its ``name`` against markdown ``![](path)``
    references. Also fills missing ``width``/``height`` by materialising
    bytes once (PIL decode), so small images can be filtered out *before*
    the upload pipeline spends S3 bandwidth.

    Returns the surviving refs sorted by position in the markdown. Refs
    smaller than ``MIN_DIMENSION`` on either axis are dropped (same
    threshold as the legacy dict-based extractor).

    Refs are mutated in place — they are the same objects returned, not
    copies — so downstream can keep the list reference stable.
    """
    by_name = {r.name: r for r in refs}
    positioned: list[tuple[ImageRef, int, int, str]] = []
    matched: set[str] = set()

    for m in _IMG_REF_RE.finditer(markdown):
        alt = m.group(1)
        ref_path = m.group(2)
        for name, ref in by_name.items():
            if name in matched:
                continue
            if name in ref_path or ref_path in name or name == ref_path:
                positioned.append((ref, m.start(), m.end(), alt))
                matched.add(name)
                break

    doc_len = len(markdown)
    for name, ref in by_name.items():
        if name not in matched:
            positioned.append((ref, doc_len, doc_len, ""))

    out: list[ImageRef] = []
    for ref, start, end, alt in positioned:
        # Lazy dimension resolution: parsers that already know dims
        # (docling) skip this cost; parsers that don't (mineru) pay a
        # single materialize+decode here rather than at upload time.
        if ref.width is None or ref.height is None:
            try:
                data = ref.materialize()
                w, h = _get_dimensions(data)
                ref.width = w
                ref.height = h
            except Exception:
                logger.exception("dimension read failed for %s", ref.name)

        if (ref.width is not None and ref.height is not None
                and (ref.width < MIN_DIMENSION or ref.height < MIN_DIMENSION)):
            # Filtered out — release so the factory's captured state
            # (base64 string / PIL.Image / docling doc) can be GC'd.
            ref.release()
            continue

        ref.alt = alt
        ref.context = _extract_context(markdown, start, end)
        ref.section_title = _find_section_title(markdown, start)
        ref.markdown_pos = start
        out.append(ref)

    return out
