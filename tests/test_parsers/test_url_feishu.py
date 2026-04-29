"""Feishu docx handler tests — mock playwright, no real network."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from parsers.url._feishu import is_feishu_docx_url


def test_is_feishu_docx_url_docx_path():
    assert is_feishu_docx_url("https://whobotai.feishu.cn/docx/ON7udn213ozGYPx8USXcRtJunFc")


def test_is_feishu_docx_url_docx_with_query():
    assert is_feishu_docx_url(
        "https://whobotai.feishu.cn/docx/ON7udn213ozGYPx8USXcRtJunFc?ignore_wx_jump=1"
    )


def test_is_feishu_docx_url_wiki_path():
    assert is_feishu_docx_url("https://example.feishu.cn/wiki/ABCdef123")


def test_is_feishu_docx_url_rejects_sheets():
    assert not is_feishu_docx_url("https://example.feishu.cn/sheets/abc")


def test_is_feishu_docx_url_rejects_drive():
    assert not is_feishu_docx_url("https://example.feishu.cn/drive/folder/abc")


def test_is_feishu_docx_url_rejects_larksuite():
    # Overseas variant out of scope for this handler
    assert not is_feishu_docx_url("https://example.larksuite.com/docx/abc")


def test_is_feishu_docx_url_rejects_other_host():
    assert not is_feishu_docx_url("https://example.com/docx/abc")
