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


def test_docling_no_longer_claims_office_types():
    from parsers.docling_parser import DoclingParser
    for ext in (".docx", ".doc", ".pptx", ".ppt", ".xlsx", ".xls"):
        assert ext not in DoclingParser.supported_types, (
            f"docling still claims {ext} — trim supported_types so "
            f"registry.supported_types() doesn't advertise Office via docling"
        )
