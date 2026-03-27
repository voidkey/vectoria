import pytest
from unittest.mock import patch, MagicMock
from parsers.url_parser import UrlParser
from parsers.base import ParseResult


@pytest.mark.asyncio
async def test_parse_url_with_trafilatura():
    with patch("parsers.url_parser.trafilatura.fetch_url", return_value="<html><body><h1>Title</h1><p>Body</p></body></html>"), \
         patch("parsers.url_parser.trafilatura.extract", return_value="Title\n\nBody"):

        parser = UrlParser()
        result = await parser.parse("https://example.com")

    assert "Body" in result.content
    assert result.title != ""


@pytest.mark.asyncio
async def test_parse_url_returns_empty_on_failure():
    with patch("parsers.url_parser.trafilatura.fetch_url", return_value=None):
        parser = UrlParser()
        result = await parser.parse("https://bad-url.example")

    assert result.content == ""


def test_engine_name():
    assert UrlParser.engine_name == "url"


def test_supported_types():
    assert "url" in UrlParser.supported_types
