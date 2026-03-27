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
