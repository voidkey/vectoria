"""Magic-byte family classifier for upload validation.

Wraps puremagic (pure-Python, no libmagic system dep) and maps detected
MIME strings to coarse-grained *families*. Uploads are validated by
comparing the family detected from the first 2 KB of bytes against the
family implied by the claimed file extension. Cross-family mismatches
(e.g. PE executable claimed as PDF) are rejected by the upload endpoint
when ``STRICT_MIME_CHECK=true``.

Families are intentionally coarse: a ``.pdf`` opened as PDF is OK, a
``.docx`` opened as a generic zip is OK (zip IS what docx is), but a
``.pdf`` that's actually PE/PKZip/HTML is rejected.
"""
from __future__ import annotations

import logging
import os

import puremagic

log = logging.getLogger(__name__)


# Extension → family. Lowercased, leading dot preserved.
EXT_FAMILIES: dict[str, str] = {
    ".pdf":  "pdf",
    ".doc":  "office-doc",
    ".docx": "office-doc",
    ".xls":  "office-sheet",
    ".xlsx": "office-sheet",
    ".ppt":  "office-slide",
    ".pptx": "office-slide",
    ".png":  "image",
    ".jpg":  "image",
    ".jpeg": "image",
    ".gif":  "image",
    ".webp": "image",
    ".bmp":  "image",
    ".tiff": "image",
    ".tif":  "image",
    ".md":   "text",
    ".txt":  "text",
    ".csv":  "text",
    ".html": "text",
    ".htm":  "text",
}


# Substrings in puremagic's mime_type / extension → family.
# The haystack is: "{mime_type} {extension}" (lowercased).
# Longer / more-specific needles are listed first so they shadow
# shorter ones (e.g. "spreadsheetml" before the bare "xls" extension).
_MIME_TO_FAMILY: list[tuple[str, str]] = [
    # PDF
    ("pdf",                          "pdf"),
    # Office documents
    ("msword",                       "office-doc"),
    ("wordprocessingml",             "office-doc"),
    ("officedocument.wordprocessing","office-doc"),
    ("ms-excel",                     "office-sheet"),
    ("spreadsheetml",                "office-sheet"),
    ("officedocument.spreadsheetml", "office-sheet"),
    ("ms-powerpoint",                "office-slide"),
    ("presentationml",               "office-slide"),
    ("officedocument.presentationml","office-slide"),
    # Images
    ("image/",                       "image"),
    ("png",                          "image"),
    ("jpeg",                         "image"),
    ("jpg",                          "image"),
    ("gif",                          "image"),
    ("bmp",                          "image"),
    ("tiff",                         "image"),
    ("webp",                         "image"),
    # Executables / PE binaries — detected so we can reject them
    # when claimed as a document type.
    ("portable-executable",          "executable"),
    ("x-dosexec",                    "executable"),
    ("x-msdownload",                 "executable"),
    (".exe",                         "executable"),
    (".dll",                         "executable"),
]


# Families we never accept regardless of claimed extension. Even if the
# user names their file ``foo.exe`` (so the extension has no declared
# family), a PE executable in the payload is still rejected here.
_BLOCKED_FAMILIES: frozenset[str] = frozenset({"executable"})


class _BytesFile:
    """Minimal file-like wrapper so puremagic can seek on in-memory head bytes."""
    def __init__(self, data: bytes):
        self._data = data
        self._pos = 0

    def read(self, n: int = -1) -> bytes:
        if n < 0:
            out, self._pos = self._data[self._pos:], len(self._data)
            return out
        out = self._data[self._pos : self._pos + n]
        self._pos += len(out)
        return out

    def seek(self, pos: int, whence: int = 0) -> int:
        if whence == 0:
            self._pos = pos
        elif whence == 1:
            self._pos += pos
        elif whence == 2:
            self._pos = len(self._data) + pos
        return self._pos

    def tell(self) -> int:
        return self._pos


def detect_family(head: bytes) -> str | None:
    """Return coarse family label for the given file head, or None (ambiguous).

    None → "couldn't identify" → callers treat as pass-through so that
    legitimate plain-text / niche formats aren't blocked.
    """
    if not head:
        return None
    try:
        matches = puremagic.magic_stream(_BytesFile(head))
    except puremagic.PureError:
        return None
    except Exception as exc:  # pragma: no cover
        log.warning("mime_sniff: puremagic raised %s", exc)
        return None

    for m in matches:
        mime = (getattr(m, "mime_type", "") or "").lower()
        ext = (getattr(m, "extension", "") or "").lower()
        haystack = f"{mime} {ext}"
        for needle, family in _MIME_TO_FAMILY:
            if needle in haystack:
                return family
    return None


def check_mime(filename: str, head: bytes) -> tuple[bool, str | None]:
    """Return ``(ok, detected_family)``.

    ``ok=True`` when detected family matches the extension family OR
    detection is ambiguous (returns None — we don't block what we
    can't identify). ``ok=False`` when:
      * detected family is in ``_BLOCKED_FAMILIES`` (executable, …) —
        rejected regardless of the claimed extension.
      * detected family is known but does not match the claimed
        extension family (e.g. PE disguised as ``.pdf``).
    """
    _, ext = os.path.splitext(filename.lower())
    claimed = EXT_FAMILIES.get(ext)
    detected = detect_family(head)

    # Unconditional reject on blocked families — an executable is never
    # a valid upload, no matter what the user named it.
    if detected in _BLOCKED_FAMILIES:
        return False, detected

    if detected is None:
        return True, None
    if claimed is None:
        return True, detected  # unknown extension, can't compare
    return (claimed == detected), detected
