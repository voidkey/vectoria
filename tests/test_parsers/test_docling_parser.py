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
    doc.pictures = []

    result = MagicMock()
    result.document = doc
    result.status = MagicMock()
    result.status.name = "SUCCESS"
    return result


@pytest.fixture(autouse=True)
def _reset_docling_converter():
    """Docling's DocumentConverter is cached in ``_converter`` at module
    level; tests that mock the class need to start from a clean slate,
    otherwise the first test's instance leaks into subsequent tests
    (post-W4-a lazy-import made this more surface).
    """
    import parsers.docling_parser as dp
    dp._converter = None
    yield
    dp._converter = None


@pytest.mark.asyncio
async def test_parse_pdf_returns_markdown(mock_docling_result):
    # Lazy import: DocumentConverter is imported inside _get_converter(),
    # so the patch targets the ORIGIN module (docling.document_converter)
    # rather than the now-unused parsers.docling_parser module attribute.
    with patch("docling.document_converter.DocumentConverter") as MockConv:
        instance = MockConv.return_value
        instance.convert.return_value = mock_docling_result

        parser = DoclingParser()
        result = await parser.parse(b"%PDF-1.4 fake content", filename="test.pdf")

    assert isinstance(result, ParseResult)
    assert "Title" in result.content
    assert result.image_refs == []


@pytest.mark.asyncio
async def test_parse_extracts_title_from_filename(mock_docling_result):
    with patch("docling.document_converter.DocumentConverter") as MockConv:
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


def test_docling_not_imported_by_docling_parser_module():
    """W4-a invariant: importing ``parsers.docling_parser`` must not
    trigger the real ``docling`` package import.

    Root of the ~400 MB baseline RSS is the chain ``docling →
    docling.models → torch → transformers``. Keeping ``docling`` off
    module-level ``from`` imports is the whole point of W4-a, so guard
    it with a subprocess test that imports the module in a fresh
    interpreter and checks ``sys.modules``.
    """
    import subprocess
    import sys
    # Running in a fresh Python so prior tests that already forced
    # docling to load don't pollute this check.
    code = (
        "import sys; import parsers.docling_parser; "
        "print('docling' in sys.modules); "
        "print('torch' in sys.modules)"
    )
    out = subprocess.check_output(
        [sys.executable, "-c", code], text=True,
    ).strip().splitlines()
    assert out[0] == "False", (
        "docling must stay out of sys.modules after importing "
        "parsers.docling_parser; some new top-level import leaked it back"
    )
    assert out[1] == "False", (
        "torch must not load transitively — the 400 MB RSS regression "
        "almost certainly came back"
    )
