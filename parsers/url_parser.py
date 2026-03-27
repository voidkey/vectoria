import asyncio
import re
from urllib.parse import urljoin, urlparse

import httpx
import trafilatura

from parsers.base import BaseParser, ParseResult

_IMG_TAG = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.IGNORECASE)


class UrlParser(BaseParser):
    engine_name = "url"
    supported_types = ["url"]

    async def parse(self, source: bytes | str, filename: str = "", **kwargs) -> ParseResult:
        url = source.decode() if isinstance(source, bytes) else source
        return await asyncio.get_running_loop().run_in_executor(None, self._parse_sync, url)

    def _parse_sync(self, url: str) -> ParseResult:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return ParseResult(content="", images={}, title="")

        text = trafilatura.extract(
            downloaded,
            include_images=True,
            include_links=False,
            output_format="markdown",
        ) or ""

        # Extract title
        title_match = re.search(r"<title[^>]*>([^<]+)</title>", downloaded, re.IGNORECASE)
        title = title_match.group(1).strip() if title_match else urlparse(url).netloc

        # Download referenced images
        images = self._download_images(downloaded, url)

        return ParseResult(content=text, images=images, title=title)

    def _download_images(self, html: str, base_url: str) -> dict[str, bytes]:
        images: dict[str, bytes] = {}
        src_list = _IMG_TAG.findall(html)
        for src in src_list[:20]:  # cap at 20 images
            try:
                abs_url = urljoin(base_url, src)
                resp = httpx.get(abs_url, timeout=10, follow_redirects=True)
                if resp.status_code == 200:
                    fname = abs_url.rsplit("/", 1)[-1].split("?")[0] or "image.jpg"
                    # deduplicate filename
                    if fname in images:
                        stem, _, ext = fname.rpartition(".")
                        fname = f"{stem}_{len(images)}.{ext}" if ext else f"{fname}_{len(images)}"
                    images[fname] = resp.content
            except Exception:
                continue
        return images
