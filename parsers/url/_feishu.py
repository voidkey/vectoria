"""Feishu docx URL handler.

Public 飞书 docx / wiki pages are SPAs — initial HTML is a React shell,
real content is rendered after JS executes. Bare httpx is redirected to
``accounts.feishu.cn`` by the anti-bot layer even for public docs.

Image URLs (``internal-api-drive-stream.feishu.cn/...``) carry no
signature token; they require the anonymous session cookie that the
docx page sets on first navigation. Bytes therefore have to be fetched
inside the same playwright ``BrowserContext`` and shipped back as
``image_refs`` so the worker takes the inline path and skips the
deferred ``download_and_store_images`` task (which uses bare httpx and
would 401).
"""
from __future__ import annotations

from urllib.parse import urlparse


_FEISHU_HOST_SUFFIX = ".feishu.cn"
_DOCX_PATH_PREFIXES = ("/docx/", "/wiki/")


def is_feishu_docx_url(url: str) -> bool:
    """True iff *url* is a 飞书 docx or wiki page on ``*.feishu.cn``.

    Other path prefixes (``/sheets/``, ``/drive/``, ``/file/`` ...) are
    not docx-shaped content and are explicitly rejected.
    """
    try:
        p = urlparse(url)
    except Exception:
        return False
    host = (p.hostname or "").lower()
    if not (host == "feishu.cn" or host.endswith(_FEISHU_HOST_SUFFIX)):
        return False
    return any(p.path.startswith(prefix) for prefix in _DOCX_PATH_PREFIXES)
