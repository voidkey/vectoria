import asyncio

import httpx

from api.errors import ErrorCode
from parsers.base import AntiBotBlockedError, PageNotFoundError
from parsers.url._blacklist import UnparseableUrlError
from worker.handlers import _parse_error_code


def test_classifier_url_timeout_is_link_timeout():
    assert _parse_error_code(httpx.ConnectTimeout("x"), is_url=True) \
        == ErrorCode.LINK_FETCH_TIMEOUT


def test_classifier_url_network_is_link_timeout():
    assert _parse_error_code(httpx.ConnectError("x"), is_url=True) \
        == ErrorCode.LINK_FETCH_TIMEOUT


def test_classifier_file_timeout_is_parse_error():
    assert _parse_error_code(asyncio.TimeoutError(), is_url=False) \
        == ErrorCode.PARSE_ERROR


def test_classifier_generic_is_parse_error():
    assert _parse_error_code(ValueError("boom"), is_url=True) \
        == ErrorCode.PARSE_ERROR


def test_classifier_permanent_uses_exception_code():
    assert _parse_error_code(AntiBotBlockedError("x"), is_url=True) \
        == ErrorCode.LINK_ANTIBOT_BLOCKED
    assert _parse_error_code(PageNotFoundError("x"), is_url=True) \
        == ErrorCode.LINK_PAGE_GONE
    assert _parse_error_code(
        UnparseableUrlError("x", code=ErrorCode.LINK_VIDEO_UNSUPPORTED),
        is_url=True) == ErrorCode.LINK_VIDEO_UNSUPPORTED
