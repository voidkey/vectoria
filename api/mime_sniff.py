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


def detect_families(head: bytes) -> list[str]:
    """Return all coarse family labels detectable in ``head``, in the
    order puremagic returns them, deduped.

    For OOXML files (.docx/.pptx/.xlsx) puremagic returns up to a dozen
    candidate matches at the same confidence — they all share the
    "ZIP-with-ContentTypes.xml" magic and only differ at the OOXML
    sub-format level. Returning the *set* lets callers verify their
    claimed extension against any of the candidates instead of
    arbitrarily latching onto the first one (which used to make
    legit .pptx uploads sniff as office-doc and get rejected).

    Empty list → puremagic identified nothing we map to a family;
    callers should pass-through (don't block what we can't classify).
    """
    if not head:
        return []
    try:
        matches = puremagic.magic_stream(_BytesFile(head))
    except puremagic.PureError:
        return []
    except Exception as exc:  # pragma: no cover
        log.warning("mime_sniff: puremagic raised %s", exc)
        return []

    families: list[str] = []
    seen: set[str] = set()
    for m in matches:
        mime = (getattr(m, "mime_type", "") or "").lower()
        ext = (getattr(m, "extension", "") or "").lower()
        haystack = f"{mime} {ext}"
        for needle, family in _MIME_TO_FAMILY:
            if needle in haystack and family not in seen:
                families.append(family)
                seen.add(family)
                break  # one family per match is enough
    return families


def detect_family(head: bytes) -> str | None:
    """Highest-priority detected family, or None.

    Convenience wrapper kept for the metric / log line that wants a
    single label. Use ``detect_families`` for any logic that needs to
    handle OOXML's multi-match ambiguity.
    """
    fams = detect_families(head)
    return fams[0] if fams else None


def check_mime(filename: str, head: bytes) -> tuple[bool, str | None]:
    """Return ``(ok, detected_family)``.

    ``ok=True`` when *any* detected family matches the extension's
    declared family — handles OOXML's docx/pptx/xlsx multi-match — OR
    when detection is ambiguous (no families recognised). ``ok=False``
    when:
      * any detected family is in ``_BLOCKED_FAMILIES`` (executable, …):
        rejected regardless of the claimed extension.
      * detected families are known but none matches the claimed
        extension (e.g. PE disguised as ``.pdf``).

    The returned ``detected_family`` is a representative label for
    metrics/logs: the blocked family if reject-by-block, else the
    first detected (which is what the legacy single-family code
    reported).
    """
    _, ext = os.path.splitext(filename.lower())
    claimed = EXT_FAMILIES.get(ext)
    detected_list = detect_families(head)

    # Unconditional reject on blocked families — an executable is never
    # a valid upload, no matter what the user named it.
    blocked = next((f for f in detected_list if f in _BLOCKED_FAMILIES), None)
    if blocked is not None:
        return False, blocked

    if not detected_list:
        return True, None
    if claimed is None:
        return True, detected_list[0]  # unknown extension, can't compare

    # Allow as long as the claim shows up anywhere in puremagic's
    # candidate list. For OOXML the list is e.g.
    # [office-doc, office-slide, office-sheet] and the claim may be
    # any one of them — we only care that the family family agrees.
    if claimed in detected_list:
        return True, claimed
    return False, detected_list[0]
