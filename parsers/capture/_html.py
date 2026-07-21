"""Self-contained page.html recreation — the high-fidelity STRUCTURAL reference.

Ported from hyperframes' htmlExtractor (packages/cli/src/capture/htmlExtractor.ts):
external stylesheets are fetched server-side (bypasses CORS) and inlined as <style>,
url()/img src are absolutized, and framework bootstrap scripts (Next.js / React
hydration) are stripped while visual-library scripts are kept. Images are left as
absolute URLs (not inlined as data URLs) — the artifact is READ as a layout/structure
reference by the video agent, so absolute refs keep it small while staying faithful.

Only produced for capture_quality == "full" (gated by the caller): a challenge /
login page's HTML must never become a "structural reference".
"""
from __future__ import annotations

import logging
import re

from parsers.capture._assets import fetch_asset_bytes

logger = logging.getLogger(__name__)

_MAX_CSS_BYTES = 2_000_000  # per-stylesheet cap
_REL_URL_RE = re.compile(r"""url\(\s*['"]?([^'")\s]+)['"]?\s*\)""")


def _rewrite_css_urls(css: str, base: str) -> str:
    """Make relative url() refs in a fetched stylesheet absolute against its href."""
    from urllib.parse import urljoin

    def repl(m: re.Match) -> str:
        u = m.group(1)
        if u.startswith(("data:", "http", "//")):
            return m.group(0)
        try:
            return f"url('{urljoin(base, u)}')"
        except Exception:
            return m.group(0)

    return _REL_URL_RE.sub(repl, css)


def _strip_framework_scripts(body: str) -> str:
    """Drop Next.js/React bootstrap payloads (huge + useless as reference); keep
    visual-library scripts. Mirrors hyperframes index.ts script-strip."""
    body = re.sub(r'<script\s+id="__NEXT_DATA__"[^>]*>[\s\S]*?</script>', "",
                  body, flags=re.IGNORECASE)
    body = re.sub(r'\s*data-reactroot(?:="[^"]*")?', "", body)

    def drop_bootstrap(m: re.Match) -> str:
        content = m.group(1)
        if ("__next_f" in content or "self.__next_f" in content
                or "__NEXT_DATA__" in content):
            return ""
        return m.group(0)

    return re.sub(r"<script\b[^>]*>([\s\S]*?)</script>", drop_bootstrap,
                  body, flags=re.IGNORECASE)


async def extract_page_html(page, *, max_css_bytes: int = _MAX_CSS_BYTES) -> str:
    """Build a self-contained page.html from the live page. Mutates the DOM
    (inlines CSS, removes <link>s) — call AFTER extract + screenshots."""
    # 1. Inline external stylesheets (fetched server-side, bypasses CORS).
    hrefs: list[str] = await page.evaluate(
        "() => Array.from(document.querySelectorAll('link[rel=\"stylesheet\"][href]'))"
        ".map(l => l.href)")
    for href in hrefs:
        got = await fetch_asset_bytes(href, max_bytes=max_css_bytes)
        if not got:
            continue
        data, _ctype = got
        try:
            css = _rewrite_css_urls(data.decode("utf-8", "ignore"), href)
        except Exception:
            continue
        try:
            await page.add_style_tag(content=css)
            await page.evaluate(
                "(h) => { for (const l of document.querySelectorAll('link[rel=\"stylesheet\"]'))"
                " { if (l.href === h) { l.remove(); break; } } }", href)
        except Exception:
            pass

    # 2. Absolutize img src / srcset (so refs resolve outside the origin).
    await page.evaluate(r"""() => {
      document.querySelectorAll('img[src]').forEach(el => {
        try { const r = el.src; if (r) el.setAttribute('src', r); } catch(e){}
      });
      document.querySelectorAll('img[srcset]').forEach(el => {
        try {
          const ss = el.getAttribute('srcset') || '';
          const fixed = ss.split(',').map(part => {
            const seg = part.trim().split(/\s+/);
            try { seg[0] = new URL(seg[0], location.href).href; } catch(e){}
            return seg.join(' ');
          }).join(', ');
          el.setAttribute('srcset', fixed);
        } catch(e){}
      });
    }""")

    # 3. Serialize head / body / <html> attributes.
    html_attrs: str = await page.evaluate(
        "() => Array.from(document.documentElement.attributes)"
        ".map(a => a.name + '=\"' + a.value + '\"').join(' ')")
    head_html: str = await page.evaluate("() => document.head ? document.head.innerHTML : ''")
    body_html: str = await page.evaluate("() => document.body ? document.body.innerHTML : ''")

    body_html = _strip_framework_scripts(body_html)
    return (f"<!doctype html>\n<html {html_attrs}>\n<head>\n{head_html}\n"
            f"</head>\n<body>\n{body_html}\n</body>\n</html>\n")
