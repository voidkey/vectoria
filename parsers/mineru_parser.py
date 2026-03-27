import base64
import re
from pathlib import Path

import httpx

from config import get_settings
from parsers.base import BaseParser, ParseResult

_B64_DATA_URI = re.compile(r"^data:image/(\w+);base64,(.+)$")
_TIMEOUT = 600.0  # large PDFs take time


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
        return bool(get_settings().mineru_api_url)

    async def parse(self, source: bytes | str, filename: str = "", **kwargs) -> ParseResult:
        if not self._api_url:
            return ParseResult(content="", images={}, title=Path(filename).stem)

        content = source if isinstance(source, bytes) else source.encode()
        data = {
            "return_md": "true",
            "return_images": "true",
            "table_enable": "true",
            "formula_enable": "true",
            "parse_method": "ocr",
            "backend": self._backend,
            "lang_list": self._language,
            "response_format_zip": "false",
        }
        files = {"files": (filename or "document.pdf", content, "application/octet-stream")}

        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(f"{self._api_url}/file_parse", data=data, files=files)
            resp.raise_for_status()
            body = resp.json()

        # Support both response schema variants (results.document / results.files)
        results = body.get("results", {})
        doc = results.get("document") or results.get("files") or {}
        md_content: str = doc.get("md_content", "")
        images_b64: dict[str, str] = doc.get("images", {})

        images = self._decode_images(images_b64)
        title = Path(filename).stem
        return ParseResult(content=md_content, images=images, title=title)

    def _decode_images(self, images_b64: dict[str, str]) -> dict[str, bytes]:
        result: dict[str, bytes] = {}
        for fname, b64str in images_b64.items():
            try:
                m = _B64_DATA_URI.match(b64str)
                raw = base64.b64decode(m.group(2) if m else b64str)
                result[fname] = raw
            except Exception:
                continue
        return result
