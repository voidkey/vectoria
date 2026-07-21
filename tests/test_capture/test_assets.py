import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from parsers.capture._assets import (
    derive_asset_name, fetch_asset_bytes, image_ref_from_bytes)


@pytest.mark.asyncio
async def test_fetch_asset_bytes_ssrf_checked():
    resp = MagicMock()
    resp.headers = {"content-type": "image/png; charset=binary"}
    fake_client = MagicMock()
    fake_client.__aenter__ = AsyncMock(return_value=fake_client)
    fake_client.__aexit__ = AsyncMock(return_value=False)
    with (
        patch("parsers.capture._assets.reresolve_and_check_ssrf", new=AsyncMock()) as ssrf,
        patch("parsers.capture._assets.make_async_client", return_value=fake_client),
        patch("parsers.capture._assets.fetch_capped",
              new=AsyncMock(return_value=(resp, b"DATA"))),
    ):
        data, ctype = await fetch_asset_bytes("https://x/logo.png", max_bytes=1000)
    ssrf.assert_awaited_once_with("https://x/logo.png")
    assert data == b"DATA" and ctype == "image/png"


@pytest.mark.asyncio
async def test_fetch_asset_bytes_ssrf_reject_returns_none():
    from api.errors import AppError, ErrorCode
    with patch("parsers.capture._assets.reresolve_and_check_ssrf",
               new=AsyncMock(side_effect=AppError(400, ErrorCode.INVALID_URL, "bad"))):
        out = await fetch_asset_bytes("http://127.0.0.1/x", max_bytes=1000)
    assert out is None


def test_derive_asset_name_alt_text_wins():
    cat = {"url": "https://x/img/12345678.jpg",
           "description": "Our Beautiful Product Shot",
           "nearestHeading": "Section Heading", "contexts": ["img[src]"]}
    assert derive_asset_name(cat, set()) == "our-beautiful-product-shot"


def test_derive_asset_name_heading_fallback_when_no_desc():
    cat = {"url": "https://x/img/abcdef123456.jpg",
           "nearestHeading": "Pricing Plans", "contexts": ["img[src]"]}
    assert derive_asset_name(cat, set()) == "pricing-plans"


def test_derive_asset_name_url_path_fallback():
    # No desc, no heading — use a meaningful URL path segment (ext stripped).
    cat = {"url": "https://x/assets/dashboard-preview.png", "contexts": ["img[src]"]}
    assert derive_asset_name(cat, set()) == "dashboard-preview"


def test_derive_asset_name_skips_hashy_url_segment():
    # Hex-hash / numeric-only path segments are not meaningful -> prefix fallback.
    cat = {"url": "https://x/a/deadbeefcafe.png", "contexts": ["img[src]"]}
    name = derive_asset_name(cat, set())
    assert name.startswith("image")


def test_derive_asset_name_section_classes_fallback():
    cat = {"url": "https://x/a/1.png",
           "sectionClasses": "hero-banner testimonial-block flex", "contexts": ["img[src]"]}
    # first two meaningful classes joined; utility classes (flex) filtered out
    assert derive_asset_name(cat, set()) == "hero-banner-testimonial-block"


def test_derive_asset_name_poster_prefix():
    cat = {"url": "https://x/a/1.png", "contexts": ["video[poster]"]}
    assert derive_asset_name(cat, set()) == "poster-0"


def test_derive_asset_name_hero_prefix_when_above_fold():
    cat = {"url": "https://x/a/2.png", "aboveFold": True, "contexts": ["img[src]"]}
    assert derive_asset_name(cat, set()) == "hero-0"


def test_derive_asset_name_image_default_prefix():
    cat = {"url": "https://x/a/3.png", "contexts": ["img[src]"]}
    assert derive_asset_name(cat, set()) == "image-0"


def test_derive_asset_name_numeric_dedup_on_collision():
    used: set[str] = set()
    cat = {"url": "https://x/a/4.png", "aboveFold": True, "contexts": ["img[src]"]}
    a = derive_asset_name(cat, used); used.add(a)
    b = derive_asset_name(cat, used); used.add(b)
    assert a == "hero-0"
    # collision on the same prefix-index resolves via numeric suffix
    assert b == "hero-1"


def test_derive_asset_name_slugifies_non_ascii_and_spaces():
    cat = {"url": "https://x/a/5.png",
           "description": "Café  Menu — Spécial!!!", "contexts": ["img[src]"]}
    assert derive_asset_name(cat, set()) == "caf-menu-spcial"


def test_derive_asset_name_empty_stable_default():
    cat = {"url": "https://x/a/6", "contexts": ["img[src]"]}
    name = derive_asset_name(cat, set())
    assert name and name.startswith("image")


def test_image_ref_from_bytes_materializes():
    ref = image_ref_from_bytes(b"HELLO", filename="logo.svg", mime="image/svg+xml",
                               width=180, height=40, alt="logo")
    assert ref.name == "logo.svg"
    assert ref.mime == "image/svg+xml"
    assert ref.width == 180
    assert ref.materialize() == b"HELLO"
