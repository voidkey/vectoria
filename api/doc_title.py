"""Placeholder titles for documents whose real title isn't known yet.

URL ingest and site capture create the Document row before anything has
been fetched, so they need a stand-in title. The obvious choice — the
URL itself — turns out to be a poor one: share links from mobile apps
append hundreds of characters of tracking parameters, which is both
unreadable in the UI and wider than the ``title`` column.
"""
from urllib.parse import urlsplit

from db.models import TITLE_MAX_LEN


def title_from_url(url: str) -> str:
    """Readable stand-in title for ``url``: host + path, no scheme,
    no query string, no fragment.

    Falls back to the raw string when there's no host to work with
    (``urlsplit`` doesn't raise on junk, it just returns empty parts).
    Never returns empty — a blank title leaves the document
    unidentifiable in the UI if the fetch later fails.
    """
    parts = urlsplit(url.strip())
    label = f"{parts.netloc}{parts.path}".rstrip("/") or url.strip()
    return label[:TITLE_MAX_LEN]
