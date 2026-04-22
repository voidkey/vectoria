import pytest
from parsers.base import ParseResult, BaseParser
from parsers.registry import ParserRegistry


class FakePdfParser(BaseParser):
    engine_name = "fake_pdf"
    supported_types = [".pdf"]

    async def parse(self, source, filename="", **kwargs):
        return ParseResult(content="# Fake PDF", images={}, title="Fake")


class FakeUrlParser(BaseParser):
    engine_name = "fake_url"
    supported_types = ["url"]

    async def parse(self, source, filename="", **kwargs):
        return ParseResult(content="# Fake URL", images={}, title="URL")


def test_register_and_get():
    reg = ParserRegistry()
    reg.register(FakePdfParser)
    parser = reg.get_by_engine("fake_pdf")
    assert isinstance(parser, FakePdfParser)


def test_get_unknown_engine_raises():
    reg = ParserRegistry()
    with pytest.raises(ValueError, match="Unknown engine"):
        reg.get_by_engine("nonexistent")


def test_auto_select_pdf():
    reg = ParserRegistry()
    reg.register(FakePdfParser)
    engine = reg.auto_select(filename="doc.pdf")
    assert engine == "fake_pdf"


def test_auto_select_url():
    reg = ParserRegistry()
    reg.register(FakeUrlParser)
    engine = reg.auto_select(url="https://example.com")
    assert engine == "fake_url"


def test_unavailable_parser_skipped_in_auto():
    class UnavailableParser(BaseParser):
        engine_name = "unavailable"
        supported_types = [".pdf"]

        @classmethod
        def is_available(cls):
            return False

        async def parse(self, source, filename="", **kwargs):
            return ParseResult(content="", images={}, title="")

    reg = ParserRegistry()
    reg.register(UnavailableParser)
    reg.register(FakePdfParser)
    engine = reg.auto_select(filename="doc.pdf")
    assert engine == "fake_pdf"


# ---------------------------------------------------------------------------
# W4-g guard: Office formats are native-only
# ---------------------------------------------------------------------------
# After the native Office migration (W4-d/e/f), docling in Office
# fallback chains was dead code (native deps are hard pins, always
# available). Pin this so a future "just add docling back as a
# fallback for safety" regression is caught — docling on Office loads
# ~400 MB of torch + transformers via lazy-import for a file that the
# native parser would also have handled.

@pytest.mark.parametrize(
    "filename,expected",
    [
        ("doc.docx",  "docx-native"),
        ("doc.doc",   "docx-native"),
        ("deck.pptx", "pptx-native"),
        ("deck.ppt",  "pptx-native"),
        ("data.xlsx", "xlsx-native"),
        ("data.xls",  "xlsx-native"),
    ],
)
def test_office_auto_select_is_native_only(filename, expected):
    from parsers.registry import registry
    assert registry.auto_select(filename=filename) == expected


def test_docling_parser_fully_removed():
    """W6-2: docling is no longer a registered parser. The PDF fallback
    slot is now ``pdfium`` (pypdfium2) and image OCR is ``ocr-native``
    (rapidocr). This test fails if someone re-adds docling without
    justifying the ~1.5 GB of torch/transformers it drags in.
    """
    from parsers.registry import registry
    engines = {cls.engine_name for cls in registry._engines.values()}
    assert "docling" not in engines, (
        "docling parser re-registered — that drags the torch stack "
        "back into the image; use pdfium / ocr-native instead"
    )


def test_docling_module_removed():
    """W6-2: parsers/docling_parser.py is deleted. Importing it should
    fail; this test makes the deletion explicit so a merge conflict
    that restores the file is caught."""
    with pytest.raises(ModuleNotFoundError):
        import parsers.docling_parser  # noqa: F401

    # Also nothing in the registry module should still import it.
    import parsers.registry
    assert "docling_parser" not in str(parsers.registry.__dict__), (
        "registry.py still imports parsers.docling_parser"
    )


