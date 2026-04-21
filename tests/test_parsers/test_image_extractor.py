"""BaseImageExtractor registry + override semantics.

The plugin framework's core promise: ``extract_override`` returns
``None`` when no extractor claims the file (parser refs preserved) and
returns the extractor's list when one matches (parser refs replaced).
Future extractors (pptx speaker notes, native PDF via pypdfium2) hang
off this seam without touching parser code.
"""
from unittest.mock import AsyncMock

import pytest

from parsers import image_extractor as ie
from parsers.image_ref import ImageRef


@pytest.fixture(autouse=True)
def _fresh_registry():
    ie._clear_for_tests()
    yield
    ie._clear_for_tests()


def _ref(name: str) -> ImageRef:
    return ImageRef(
        name=name, mime="image/png",
        _factory=lambda: b"x",
    )


class _StubExtractor:
    def __init__(self, ext_match: str, refs: list[ImageRef]):
        self.ext_match = ext_match
        self.refs = refs
        self.extract = AsyncMock(return_value=refs)

    def match(self, *, mime: str = "", ext: str = "") -> bool:
        return ext == self.ext_match


# ---------------------------------------------------------------------------
# Registry / lookup
# ---------------------------------------------------------------------------

def test_find_returns_none_when_registry_empty():
    assert ie.find_image_extractor(ext=".pptx") is None


def test_find_matches_by_extension():
    e = _StubExtractor(".pptx", [_ref("slide-1.png")])
    ie.register_image_extractor(e)
    assert ie.find_image_extractor(ext=".pptx") is e
    assert ie.find_image_extractor(ext=".docx") is None


def test_later_registration_overrides_earlier():
    """Reverse lookup order lets ops inject a specialised extractor
    at test or runtime without removing the default first.
    """
    default = _StubExtractor(".pptx", [_ref("default.png")])
    override = _StubExtractor(".pptx", [_ref("override.png")])
    ie.register_image_extractor(default)
    ie.register_image_extractor(override)
    assert ie.find_image_extractor(ext=".pptx") is override


def test_match_exception_is_swallowed_and_next_tried():
    """A buggy match() on one extractor must not poison lookups for
    the others. This is the anti-foot-shooting guard.
    """
    class _Exploding:
        def match(self, *, mime="", ext=""):
            raise RuntimeError("boom")

        async def extract(self, src, *, filename=""):
            return []

    good = _StubExtractor(".pptx", [_ref("ok.png")])
    ie.register_image_extractor(good)
    ie.register_image_extractor(_Exploding())

    # Reverse order hits the exploder first; must fall through to good.
    assert ie.find_image_extractor(ext=".pptx") is good


# ---------------------------------------------------------------------------
# extract_override integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_extract_override_returns_none_when_no_match():
    """None is the signal for 'keep parser refs' — empty list would
    mean 'extractor ran and found nothing', which is a different
    semantic.
    """
    assert await ie.extract_override(b"bytes", filename="x.pdf") is None


@pytest.mark.asyncio
async def test_extract_override_returns_extractor_list_when_matched():
    refs = [_ref("a.png"), _ref("b.png")]
    e = _StubExtractor(".pptx", refs)
    ie.register_image_extractor(e)

    result = await ie.extract_override(b"pptx bytes", filename="deck.pptx")
    assert result == refs
    e.extract.assert_awaited_once_with(b"pptx bytes", filename="deck.pptx")


@pytest.mark.asyncio
async def test_extract_override_empty_list_is_valid():
    """``[]`` means the extractor ran and found no images — distinct
    from ``None`` which means no extractor matched.
    """
    e = _StubExtractor(".pptx", [])
    ie.register_image_extractor(e)
    result = await ie.extract_override(b"", filename="empty.pptx")
    assert result == []  # not None


@pytest.mark.asyncio
async def test_extract_override_swallows_extract_exception():
    """A failing extractor must not break the ingest — fall through to
    ``None`` so the parser's own refs are used.
    """
    class _Broken:
        def match(self, *, mime="", ext=""): return ext == ".pptx"
        async def extract(self, source, *, filename=""):
            raise RuntimeError("dead")

    ie.register_image_extractor(_Broken())
    result = await ie.extract_override(b"x", filename="x.pptx")
    assert result is None


@pytest.mark.asyncio
async def test_extract_override_ignores_missing_extension():
    """Files without an extension (or dot-less filenames) simply don't
    match extractors; return None. Mime would be the discriminator in
    that case — extractors that care set ``match`` accordingly.
    """
    e = _StubExtractor(".pptx", [_ref("x.png")])
    ie.register_image_extractor(e)
    assert await ie.extract_override(b"x", filename="noext") is None
