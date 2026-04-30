import json
import logging
import re
from pathlib import Path, PurePosixPath

import httpx

from config import get_settings
from infra.circuit_breaker import CircuitOpenError, get_breaker
from parsers.base import BaseParser, ParseResult
from parsers.image_ref import Base64Factory, ImageRef

logger = logging.getLogger(__name__)

_B64_DATA_URI = re.compile(r"^data:image/(\w+);base64,(.+)$")
# Timeouts chosen to keep a hung mineru from pinning the ingest semaphore
# slot for 10 minutes. 120 s read/write covers realistic PDFs; connect is
# snug because mineru runs in the same DC.
_TIMEOUT = httpx.Timeout(120.0, connect=10.0)


async def _call_mineru_api(api_url: str, data: dict, files: dict) -> dict:
    """HTTP request extracted so the circuit breaker can wrap it without
    reaching inside the parser class.
    """
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.post(f"{api_url}/file_parse", data=data, files=files)
        resp.raise_for_status()
        return resp.json()


class MinerUParser(BaseParser):
    engine_name = "mineru"
    supported_types = [".pdf"]

    def __init__(self, api_url: str | None = None):
        cfg = get_settings()
        self._api_url = (api_url or cfg.mineru_api_url).rstrip("/")
        self._backend = cfg.mineru_backend
        self._language = cfg.mineru_language

    @classmethod
    def is_available(cls) -> bool:
        # If the URL is unset, there is nothing to call — short-circuit
        # before touching the breaker so we don't create an instance we
        # would never use.
        if not get_settings().mineru_api_url:
            return False
        # Circuit OPEN → advertise as unavailable so parsers.registry
        # falls back to the next candidate (docling) in the preference
        # list rather than returning an empty ParseResult that upstream
        # would reject as EMPTY_CONTENT. HALF_OPEN stays "available" so
        # one probe request can reach the dependency and potentially
        # close the circuit.
        from infra.circuit_breaker import State, get_breaker
        return get_breaker("mineru").current_state() is not State.OPEN

    async def parse(self, source: bytes | str, filename: str = "", **kwargs) -> ParseResult:
        if not self._api_url:
            return ParseResult(content="", title=Path(filename).stem)

        content = source if isinstance(source, bytes) else source.encode()
        data = {
            "return_md": "true",
            "return_images": "true",
            # content_list is the only response field that carries
            # per-image page numbers (md_content has no page anchors and
            # the images dict is page-less). Cheap to ask for; we only
            # walk it to build a fname→page map.
            "return_content_list": "true",
            "table_enable": "true",
            "formula_enable": "true",
            "parse_method": "ocr",
            "backend": self._backend,
            "lang_list": self._language,
            "response_format_zip": "false",
        }
        files = {"files": ("document.pdf", content, "application/octet-stream")}

        try:
            body = await get_breaker("mineru").call(
                _call_mineru_api, self._api_url, data, files,
            )
        except CircuitOpenError:
            # Mineru proven unhealthy — return empty so the caller treats
            # this exactly like "mineru not configured" (the existing
            # empty-url path above). Upstream will surface EMPTY_CONTENT
            # in ~milliseconds instead of hanging for 120 s.
            logger.warning(
                "MinerU circuit open; returning empty result for %s",
                filename,
            )
            return ParseResult(content="", title=Path(filename).stem)

        # Support both response schema variants:
        # - legacy: results.document / results.files
        # - current: results.<filename_stem>
        results = body.get("results", {})
        doc = results.get("document") or results.get("files") or next(iter(results.values()), {})
        md_content: str = doc.get("md_content", "")
        images_b64: dict[str, str] = doc.get("images", {})

        page_map = _page_map_from_content_list(doc.get("content_list"))
        image_refs = self._build_image_refs(images_b64, page_map)
        title = Path(filename).stem
        return ParseResult(content=md_content, title=title, image_refs=image_refs)

    def _build_image_refs(
        self,
        images_b64: dict[str, str],
        page_map: dict[str, int],
    ) -> list[ImageRef]:
        """Build lazy refs without decoding.

        Each factory captures its base64 string and decodes on demand.
        Holding the b64 string is ~1.33× the decoded byte size, but we
        never hold both at once: the decoded bytes live only from
        ``materialize()`` through the single upload call before
        ``release()`` drops the b64 capture too.

        ``page_map`` is keyed by the image's basename — content_list
        records ``img_path`` like ``"images/fig1.png"`` while the
        ``images`` dict keys may be either form depending on backend
        version. Basename match papers over that.
        """
        refs: list[ImageRef] = []
        for fname, b64str in images_b64.items():
            # mime sniffed from data URI prefix when present; plain
            # base64 strings assume PNG (MinerU's documented output).
            m = _B64_DATA_URI.match(b64str)
            if m:
                mime = f"image/{m.group(1).lower()}"
                payload = m.group(2)
            else:
                mime = "image/png"
                payload = b64str
            refs.append(ImageRef(
                name=fname, mime=mime,
                page=page_map.get(PurePosixPath(fname).name),
                _factory=Base64Factory(payload),
            ))
        return refs


def _page_map_from_content_list(content_list) -> dict[str, int]:
    """Build basename→1-based-page from MinerU's ``content_list``.

    MinerU returns ``content_list`` as a **JSON-encoded string** (not a
    parsed list) — iterating it directly walks characters and silently
    yields an empty map. Decode if needed; tolerate a real list too in
    case a future backend version inlines it.

    Entries with ``img_path`` look like
    ``{"img_path": "images/fig1.png", "page_idx": 0, ...}``; ``page_idx``
    is 0-based, exposed as 1-based to match human page numbering. Items
    missing either field are skipped — schema-variant tolerance, not a
    silent error.
    """
    if isinstance(content_list, str):
        try:
            content_list = json.loads(content_list)
        except (ValueError, TypeError):
            return {}
    if not isinstance(content_list, list):
        return {}
    out: dict[str, int] = {}
    for item in content_list:
        if not isinstance(item, dict):
            continue
        path = item.get("img_path")
        page_idx = item.get("page_idx")
        if not path or not isinstance(page_idx, int):
            continue
        out[PurePosixPath(path).name] = page_idx + 1
    return out
