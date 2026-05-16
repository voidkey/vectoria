"""PaddleOCR-VL gateway parser.

Hits the VL layout-parsing endpoint with a base64-encoded PDF and turns
the per-page ``layoutParsingResults`` into a single ``ParseResult``:
markdown text concatenated by page, with lazy ``ImageRef`` factories for
every image the gateway returns. Page numbers are derived from the
array position (VL returns one element per page in order), which is
strictly cleaner than MinerU's ``content_list``-keyed mapping.

VL's markdown uses HTML ``<img src="path">`` tags rather than markdown
``![](path)`` — we rewrite to the markdown form here so the shared
``parsers.image_metadata`` extractor (markdown-only regex) finds the
references, and so chunked text feeding embeddings doesn't carry HTML
noise.

Concurrency: VL is a single-GPU gateway and serializes inference; >3
concurrent image-heavy requests has been observed to drop connections.
A module-level semaphore bounds in-process concurrency to
``cfg.paddle_concurrency``. Multi-worker hosts get N × ceiling; tune
at worker count, not here.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import re
from pathlib import Path, PurePosixPath

import httpx

from config import get_settings
from infra.circuit_breaker import CircuitOpenError, get_breaker
from parsers.base import BaseParser, ParseResult
from parsers.image_ref import Base64Factory, ImageRef

logger = logging.getLogger(__name__)

# <img src="path" ...> in either quote style; greedy on attributes but
# anchored on the ``src`` value so other attrs (alt, width) don't bleed
# into the captured path. Self-closing slash is optional — VL emits both.
_IMG_TAG_RE = re.compile(
    r'<img\b[^>]*\bsrc=["\']([^"\']+)["\'][^>]*/?>',
    re.IGNORECASE,
)

_MIME_BY_EXT = {
    ".png":  "image/png",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif":  "image/gif",
    ".bmp":  "image/bmp",
}

# Single shared semaphore; recreated only if the configured limit changes
# (tests / hot-reload). Lives at module scope so all PaddleParser
# instances in the process share one budget.
_VL_SEMAPHORE: asyncio.Semaphore | None = None
_VL_SEM_SIZE: int = 0


def _get_semaphore(limit: int) -> asyncio.Semaphore:
    global _VL_SEMAPHORE, _VL_SEM_SIZE
    if _VL_SEMAPHORE is None or _VL_SEM_SIZE != limit:
        _VL_SEMAPHORE = asyncio.Semaphore(limit)
        _VL_SEM_SIZE = limit
    return _VL_SEMAPHORE


async def _call_paddle_api(
    api_url: str, headers: dict, body: dict, timeout: httpx.Timeout,
) -> dict:
    """HTTP request extracted so the circuit breaker can wrap it without
    reaching inside the parser class. Matches the shape of
    ``parsers.mineru_parser._call_mineru_api`` so the breaker integration
    pattern is identical across PDF backends.
    """
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            f"{api_url}/layout-parsing", headers=headers, json=body,
        )
        resp.raise_for_status()
        return resp.json()


class PaddleParser(BaseParser):
    engine_name = "paddle"
    supported_types = [".pdf"]

    def __init__(self, api_url: str | None = None, api_key: str | None = None):
        cfg = get_settings()
        self._api_url = (api_url or cfg.paddle_api_url).rstrip("/")
        self._api_key = api_key or cfg.paddle_api_key.get_secret_value()
        self._timeout = httpx.Timeout(cfg.paddle_timeout, connect=10.0)
        self._concurrency = cfg.paddle_concurrency

    @classmethod
    def is_available(cls) -> bool:
        # Both URL and key required: missing either yields 401/connect-
        # refused at call time which would burn the breaker on a pure
        # config mistake. Short-circuit so the registry falls straight
        # to mineru.
        cfg = get_settings()
        if not cfg.paddle_api_url or not cfg.paddle_api_key.get_secret_value():
            return False
        from infra.circuit_breaker import State, get_breaker
        return get_breaker("paddle").current_state() is not State.OPEN

    async def parse(
        self, source: bytes | str, filename: str = "", **kwargs,
    ) -> ParseResult:
        if not self._api_url or not self._api_key:
            return ParseResult(content="", title=Path(filename).stem)

        content = source if isinstance(source, bytes) else source.encode()
        body = {
            "file": base64.b64encode(content).decode("ascii"),
            "fileType": 0,  # 0 = PDF; image files (fileType=1) are routed
                            # by the registry to vision-native/ocr-native
                            # parsers, not here.
        }
        headers = {"X-API-Key": self._api_key}

        try:
            # Semaphore wraps the breaker call rather than the inner HTTP:
            # the breaker's fail-fast still gets to run inside the slot,
            # but we only ever hold a slot while actually waiting on VL.
            async with _get_semaphore(self._concurrency):
                payload = await get_breaker("paddle").call(
                    _call_paddle_api, self._api_url, headers, body, self._timeout,
                )
        except CircuitOpenError:
            # Same shape as MinerUParser's open-circuit handling — caller
            # treats this exactly like "paddle not configured" and the
            # ingest handler's per-attempt chain fallback moves on to
            # mineru in milliseconds rather than burning the 600 s
            # client timeout per attempt.
            logger.warning(
                "Paddle circuit open; returning empty result for %s", filename,
            )
            return ParseResult(content="", title=Path(filename).stem)

        # HTTP 200 with errorCode != 0 = business-level failure (most
        # commonly "format unsupported"). Raise so the handler falls
        # back to the next engine in the chain; another engine may
        # accept the file via a different code path. errorMsg goes
        # into the exception so logs are grep-able by VL's wording.
        if payload.get("errorCode") != 0:
            raise RuntimeError(
                f"paddle errorCode={payload.get('errorCode')}: "
                f"{(payload.get('errorMsg') or '')[:200]}",
            )

        pages = (payload.get("result") or {}).get("layoutParsingResults") or []
        md_parts: list[str] = []
        image_refs: list[ImageRef] = []
        for page_idx, page in enumerate(pages, start=1):
            md_block = page.get("markdown") or {}
            text = md_block.get("text", "")
            images = md_block.get("images") or {}
            # VL emits <img src="imgs/img_in_image_box_..."> — rewrite to
            # ![](basename) so (a) the markdown image extractor matches,
            # and (b) chunked text doesn't carry HTML into embeddings.
            text = _IMG_TAG_RE.sub(
                lambda m: f"![]({PurePosixPath(m.group(1)).name})", text,
            )
            md_parts.append(text)
            for rel_path, b64str in images.items():
                fname = PurePosixPath(rel_path).name
                ext = ("." + fname.rsplit(".", 1)[-1]).lower() if "." in fname else ""
                mime = _MIME_BY_EXT.get(ext, "image/png")
                image_refs.append(ImageRef(
                    name=fname, mime=mime, page=page_idx,
                    _factory=Base64Factory(b64str),
                ))

        # Empty pages still contribute a blank section between siblings —
        # filter so the join doesn't double-newline before non-empty text.
        markdown = "\n\n".join(p for p in md_parts if p)
        return ParseResult(
            content=markdown,
            title=Path(filename).stem,
            image_refs=image_refs,
            page_count=len(pages) or None,
        )
