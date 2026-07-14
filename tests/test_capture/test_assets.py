import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from parsers.capture._assets import fetch_asset_bytes, image_ref_from_bytes


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


def test_image_ref_from_bytes_materializes():
    ref = image_ref_from_bytes(b"HELLO", filename="logo.svg", mime="image/svg+xml",
                               width=180, height=40, alt="logo")
    assert ref.name == "logo.svg"
    assert ref.mime == "image/svg+xml"
    assert ref.width == 180
    assert ref.materialize() == b"HELLO"
