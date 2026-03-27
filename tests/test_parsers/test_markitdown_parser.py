import pytest
from unittest.mock import patch, MagicMock
from parsers.markitdown_parser import MarkitdownParser


@pytest.mark.asyncio
async def test_parse_returns_text_content():
    mock_result = MagicMock()
    mock_result.text_content = "# Hello\n\nWorld"

    with patch("parsers.markitdown_parser._AVAILABLE", True), \
         patch("parsers.markitdown_parser.MarkItDown", create=True) as MockMD:
        MockMD.return_value.convert.return_value = mock_result
        parser = MarkitdownParser()
        result = await parser.parse(b"some content", filename="doc.md")

    assert "Hello" in result.content
    assert result.title == "doc"
    assert result.images == {}


@pytest.mark.asyncio
async def test_parse_handles_conversion_error():
    with patch("parsers.markitdown_parser._AVAILABLE", True), \
         patch("parsers.markitdown_parser.MarkItDown", create=True) as MockMD:
        MockMD.return_value.convert.side_effect = RuntimeError("conversion failed")
        parser = MarkitdownParser()
        result = await parser.parse(b"bad content", filename="bad.pdf")

    assert result.content == ""


def test_engine_name():
    assert MarkitdownParser.engine_name == "markitdown"


def test_is_available_false_when_not_installed():
    with patch("parsers.markitdown_parser._AVAILABLE", False):
        assert MarkitdownParser.is_available() is False


def test_is_available_true_when_installed():
    with patch("parsers.markitdown_parser._AVAILABLE", True):
        assert MarkitdownParser.is_available() is True
