import pytest
from parsers.base import ParseResult, BaseParser
from parsers.registry import ParserRegistry


class FakePdfParser(BaseParser):
    engine_name = "fake_pdf"
    supported_types = [".pdf"]

    async def parse(self, source, filename="", **kwargs):
        return ParseResult(content="# Fake PDF", title="Fake")


class FakeUrlParser(BaseParser):
    engine_name = "fake_url"
    supported_types = ["url"]

    async def parse(self, source, filename="", **kwargs):
        return ParseResult(content="# Fake URL", title="URL")


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
            return ParseResult(content="", title="")

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




def test_fallback_chain_pdf_full():
    """For .pdf the natural chain is paddle → pdfium → markitdown.
    mineru is registered as a parser but deliberately not in the
    chain — see _EXT_PREFERENCE comment in parsers/registry.py."""
    from parsers.registry import registry
    chain = registry.fallback_chain(filename="x.pdf")
    assert chain == ["paddle", "pdfium", "markitdown"]


def test_fallback_chain_after_drops_engines_up_to_and_including():
    from parsers.registry import registry
    chain = registry.fallback_chain(filename="x.pdf", after="paddle")
    assert chain == ["pdfium", "markitdown"]
    chain = registry.fallback_chain(filename="x.pdf", after="pdfium")
    assert chain == ["markitdown"]
    chain = registry.fallback_chain(filename="x.pdf", after="markitdown")
    assert chain == []


def test_fallback_chain_after_unknown_engine_returns_empty():
    """Defensive: if ``after`` names an engine that's not in the
    chain at all (typo, removed engine), return [] rather than the
    full chain — that way the worker doesn't accidentally retry
    everything when the caller's intent was 'continue past X'."""
    from parsers.registry import registry
    assert registry.fallback_chain(filename="x.pdf", after="bogus") == []


def test_fallback_chain_url_is_just_url():
    from parsers.registry import registry
    assert registry.fallback_chain(url="https://x.test") == ["url"]
    # url=...+after="url" → empty
    assert registry.fallback_chain(url="https://x.test", after="url") == []


def test_fallback_chain_office_falls_back_to_markitdown():
    """Office formats now have markitdown as last-resort fallback —
    one rare-shape bug in python-pptx / mammoth / openpyxl no longer
    kills the file outright. The native parser is still primary
    (better fidelity); markitdown only runs when native raises.
    """
    from parsers.registry import registry
    assert registry.fallback_chain(filename="deck.pptx") == ["pptx-native", "markitdown"]
    assert registry.fallback_chain(filename="paper.docx") == ["docx-native", "markitdown"]
    assert registry.fallback_chain(filename="sheet.xlsx") == ["xlsx-native", "markitdown"]
    # ``after=`` still rotates the chain correctly.
    assert registry.fallback_chain(filename="deck.pptx", after="pptx-native") == ["markitdown"]
    assert registry.fallback_chain(filename="deck.pptx", after="markitdown") == []
