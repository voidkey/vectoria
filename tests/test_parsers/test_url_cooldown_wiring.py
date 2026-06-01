import pytest
from unittest.mock import AsyncMock, patch

from parsers.base import AntiBotBlockedError, ParseResult
from parsers.url import UrlParser


@pytest.mark.asyncio
async def test_blocked_domain_short_circuits():
    with (
        patch("parsers.url.reresolve_and_check_ssrf", new=AsyncMock()),
        patch("parsers.url.is_blocked", new=AsyncMock(return_value=True)),
        patch("parsers.url.find_handler") as find,
    ):
        with pytest.raises(AntiBotBlockedError):
            await UrlParser().parse("https://blocked.example.com/x")
        find.assert_not_called()


@pytest.mark.asyncio
async def test_terminal_block_marks_cooldown():
    handler = AsyncMock()
    handler.parse = AsyncMock(side_effect=AntiBotBlockedError("blocked at url"))
    mark = AsyncMock()
    with (
        patch("parsers.url.reresolve_and_check_ssrf", new=AsyncMock()),
        patch("parsers.url.is_blocked", new=AsyncMock(return_value=False)),
        patch("parsers.url.mark_blocked", new=mark),
        patch("parsers.url.find_handler", return_value=handler),
    ):
        with pytest.raises(AntiBotBlockedError):
            await UrlParser().parse("https://hostile.example.com/y")
    mark.assert_awaited_once_with("hostile.example.com")


@pytest.mark.asyncio
async def test_normal_parse_no_cooldown_interaction():
    handler = AsyncMock()
    handler.parse = AsyncMock(return_value=ParseResult(content="ok", title="t"))
    mark = AsyncMock()
    with (
        patch("parsers.url.reresolve_and_check_ssrf", new=AsyncMock()),
        patch("parsers.url.is_blocked", new=AsyncMock(return_value=False)),
        patch("parsers.url.mark_blocked", new=mark),
        patch("parsers.url.find_handler", return_value=handler),
    ):
        res = await UrlParser().parse("https://ok.example.com/z")
    assert res.content == "ok"
    mark.assert_not_awaited()
