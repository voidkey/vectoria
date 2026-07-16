from api.errors import ErrorCode
from parsers.base import (
    AntiBotBlockedError, PageNotFoundError, PermanentParseError,
)
from parsers.url._blacklist import BlacklistHandler, UnparseableUrlError


def test_base_permanent_defaults_to_unresolvable():
    assert PermanentParseError("x").error_code == ErrorCode.PARSE_UNRESOLVABLE


def test_antibot_code():
    assert AntiBotBlockedError("x").error_code == ErrorCode.LINK_ANTIBOT_BLOCKED


def test_page_not_found_code():
    assert PageNotFoundError("x").error_code == ErrorCode.LINK_PAGE_GONE


def test_unparseable_carries_explicit_code():
    assert UnparseableUrlError("x", code=ErrorCode.LINK_VIDEO_UNSUPPORTED).error_code \
        == ErrorCode.LINK_VIDEO_UNSUPPORTED


def test_unparseable_defaults_to_unresolvable():
    assert UnparseableUrlError("x").error_code == ErrorCode.PARSE_UNRESOLVABLE


async def test_blacklist_video_raises_video_code():
    h = BlacklistHandler()
    try:
        await h.parse("https://www.youtube.com/watch?v=abc")
    except UnparseableUrlError as e:
        assert e.error_code == ErrorCode.LINK_VIDEO_UNSUPPORTED
    else:
        raise AssertionError("expected UnparseableUrlError")


async def test_blacklist_region_raises_region_code(monkeypatch):
    monkeypatch.setattr(
        "parsers.url._blacklist._unreachable_suffixes",
        lambda: ("blocked.example",),
    )
    h = BlacklistHandler()
    try:
        await h.parse("https://blocked.example/page")
    except UnparseableUrlError as e:
        assert e.error_code == ErrorCode.LINK_REGION_BLOCKED
    else:
        raise AssertionError("expected UnparseableUrlError")
