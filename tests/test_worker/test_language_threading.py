"""Task 4: verify that `language` is threaded through the parse and image
analysis pipelines.

Tests confirm:
  1. handle_parse_document passes `language` from payload to parser.parse.
  2. handle_analyze_images passes `language` from payload to client.describe.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from parsers.base import ParseResult


def _build_session(doc):
    session = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = doc
    session.execute = AsyncMock(return_value=result)
    return session


@pytest.mark.asyncio
async def test_parse_document_passes_language_to_parser():
    """handle_parse_document must forward payload['language'] to parser.parse."""
    captured: dict = {}

    class _Parser:
        engine_name = "vision-native"

        async def parse(self, raw, filename="", **kwargs):
            captured["language"] = kwargs.get("language")
            return ParseResult(
                content="a sufficiently long body for indexing " * 3,
                title="t",
            )

    doc = MagicMock()
    doc.status = "queued"

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.get_storage") as mock_storage,
        patch("worker.handlers.update_doc", new=AsyncMock()),
        patch("worker.queue.enqueue", new=AsyncMock()),
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.return_value = _Parser()
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b"data"))

        import worker.handlers as h
        payload = {
            "doc_id": "d1", "kb_id": "k1",
            "storage_key": "sk",
            "source": "x.png", "filename": "x.png",
            "selected_engine": "vision-native",
            "language": "pt-BR",
        }
        try:
            await h.handle_parse_document(payload)
        except Exception:
            pass  # downstream mocks may not be complete; only care that parse was called

    assert captured.get("language") == "pt-BR", (
        f"Expected language='pt-BR' to reach parser.parse; got {captured!r}"
    )


@pytest.mark.asyncio
async def test_parse_document_language_none_when_absent():
    """If language is absent from payload, parser.parse receives language=None
    (backward compatible — existing tasks without language still work)."""
    captured: dict = {}

    class _Parser:
        engine_name = "markitdown"

        async def parse(self, raw, filename="", **kwargs):
            captured["language"] = kwargs.get("language", "MISSING")
            return ParseResult(
                content="enough content to pass the threshold " * 3,
                title="t",
            )

    doc = MagicMock()
    doc.status = "queued"

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.get_storage") as mock_storage,
        patch("worker.handlers.update_doc", new=AsyncMock()),
        patch("worker.queue.enqueue", new=AsyncMock()),
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.return_value = _Parser()
        mock_storage.return_value = AsyncMock(get=AsyncMock(return_value=b"data"))

        import worker.handlers as h
        payload = {
            "doc_id": "d2", "kb_id": "k1",
            "storage_key": "sk",
            "source": "x.pdf", "filename": "x.pdf",
            "selected_engine": "markitdown",
            # no "language" key
        }
        try:
            await h.handle_parse_document(payload)
        except Exception:
            pass

    assert captured.get("language") is None, (
        f"Expected language=None when absent from payload; got {captured!r}"
    )


@pytest.mark.asyncio
async def test_analyze_images_passes_language_to_describe():
    """handle_analyze_images must forward payload['language'] to client.describe."""
    from db.models import DocumentImage

    captured: dict = {}

    fake_img = MagicMock(spec=DocumentImage)
    fake_img.id = "img-1"
    fake_img.storage_key = "kb/doc/img-1.jpg"
    fake_img.context = "some context"
    fake_img.section_title = "Sec"
    fake_img.alt = "alt text"

    # Session mock: first call (pending query) returns [fake_img];
    # second call (update record) returns the same record.
    call_count = 0

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        async def execute(self, stmt):
            nonlocal call_count
            call_count += 1
            result = MagicMock()
            if call_count == 1:
                result.scalars.return_value.all.return_value = [fake_img]
            else:
                result.scalar_one_or_none.return_value = fake_img
            return result

        async def commit(self):
            pass

    async def _fake_describe(image_bytes, *, context="", section_title="", alt="", language=None):
        captured["language"] = language
        return "a description"

    fake_client = MagicMock()
    fake_client.is_configured = True
    fake_client.describe = _fake_describe

    fake_storage = AsyncMock()
    fake_storage.get = AsyncMock(return_value=b"\xff\xd8\xff")  # jpeg magic bytes

    # handle_analyze_images does `from vision.client import VisionClient` and
    # `from storage import get_storage` inside the function body, so we patch
    # at those module locations, not worker.handlers.
    with (
        patch("worker.handlers.get_session", return_value=_FakeSession()),
        patch("vision.client.VisionClient", return_value=fake_client),
        patch("storage.get_storage", return_value=fake_storage),
    ):
        import worker.handlers as h
        await h.handle_analyze_images({
            "kb_id": "k1",
            "doc_id": "d1",
            "language": "zh-CN",
        })

    assert captured.get("language") == "zh-CN", (
        f"Expected language='zh-CN' to reach client.describe; got {captured!r}"
    )
