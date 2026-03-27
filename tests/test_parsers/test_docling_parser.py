import pytest
from unittest.mock import patch, MagicMock
from parsers.docling_parser import DoclingParser
from parsers.base import ParseResult


@pytest.fixture
def mock_docling_result():
    """Simulate Docling's DocumentConverter output."""
    doc = MagicMock()
    doc.export_to_markdown.return_value = "# Title\n\nSome content."
    doc.pages = []

    result = MagicMock()
    result.document = doc
    result.status = MagicMock()
    result.status.name = "SUCCESS"
    return result


@pytest.mark.asyncio
async def test_parse_pdf_returns_markdown(mock_docling_result):
    with patch("parsers.docling_parser.DocumentConverter") as MockConv:
        instance = MockConv.return_value
        instance.convert.return_value = mock_docling_result

        parser = DoclingParser()
        result = await parser.parse(b"%PDF-1.4 fake content", filename="test.pdf")

    assert isinstance(result, ParseResult)
    assert "Title" in result.content
    assert result.images == {}


@pytest.mark.asyncio
async def test_parse_extracts_title_from_filename(mock_docling_result):
    with patch("parsers.docling_parser.DocumentConverter") as MockConv:
        instance = MockConv.return_value
        instance.convert.return_value = mock_docling_result

        parser = DoclingParser()
        result = await parser.parse(b"fake docx", filename="my_report.docx")

    assert result.title == "my_report"


def test_engine_name():
    assert DoclingParser.engine_name == "docling"


def test_supported_types():
    assert ".pdf" in DoclingParser.supported_types
    assert ".docx" in DoclingParser.supported_types
