"""``page_count`` plumbing across upload → DB → detail API.

PDF/PPTX uploads already pay for ``count_pdf_pages`` / ``count_pptx_slides``
at the size gate; this test suite pins the contract that the value is
also persisted on the Document row and surfaced by GET /documents/{id}.
Other formats (txt, docx) must leave page_count NULL — Word's notion of
"page" is render-time, and there's no honest static answer.
"""
import pytest
from datetime import datetime
from unittest.mock import patch, AsyncMock, MagicMock

from db.models import Document


def _mock_session_with_capture(adds: list[object]):
    session = AsyncMock()
    miss = MagicMock()
    miss.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=miss)

    def _refresh(obj):
        obj.created_at = datetime(2026, 5, 14, 12, 0, 0)

    session.add = MagicMock(side_effect=lambda obj: adds.append(obj))
    session.commit = AsyncMock()
    session.refresh = AsyncMock(side_effect=_refresh)
    return session


@pytest.mark.asyncio
async def test_pdf_upload_persists_page_count(client):
    """PDF upload writes the gate-counted page total to Document.page_count."""
    adds: list[object] = []
    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("api.pdf_inspect.count_pdf_pages", return_value=42),
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "mineru"
        mock_sess.return_value.__aenter__.return_value = _mock_session_with_capture(adds)

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("ok.pdf", b"%PDF-1.4\nfake", "application/pdf")},
        )

    assert resp.status_code == 201, resp.text
    doc_rows = [o for o in adds if isinstance(o, Document)]
    assert len(doc_rows) == 1
    assert doc_rows[0].page_count == 42


@pytest.mark.asyncio
async def test_pptx_upload_persists_page_count(client):
    """PPTX upload writes the gate-counted slide total to Document.page_count."""
    adds: list[object] = []
    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("api.pptx_inspect.count_pptx_slides", return_value=17),
        patch("api.mime_sniff.check_mime", return_value=(True, "office_zip")),
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "pptx"
        mock_sess.return_value.__aenter__.return_value = _mock_session_with_capture(adds)

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": (
                "deck.pptx",
                b"PK\x03\x04 fake",
                "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            )},
        )

    assert resp.status_code == 201, resp.text
    doc_rows = [o for o in adds if isinstance(o, Document)]
    assert len(doc_rows) == 1
    assert doc_rows[0].page_count == 17


@pytest.mark.asyncio
async def test_non_paginated_upload_leaves_page_count_null(client):
    """``.txt`` (and other non-paginated formats) skip the gate, so
    page_count stays NULL — distinguishing "no value" from "0 pages".
    """
    adds: list[object] = []
    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        patch("api.routes.documents.get_session") as mock_sess,
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "markitdown"
        mock_sess.return_value.__aenter__.return_value = _mock_session_with_capture(adds)

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("notes.txt", b"hello world", "text/plain")},
        )

    assert resp.status_code == 201, resp.text
    doc_rows = [o for o in adds if isinstance(o, Document)]
    assert len(doc_rows) == 1
    assert doc_rows[0].page_count is None


@pytest.mark.asyncio
async def test_corrupt_pdf_leaves_page_count_null(client):
    """count_pdf_pages returns ``None`` on unloadable bytes — gate
    fails open and lets the doc through, page_count stays NULL.
    """
    adds: list[object] = []
    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("api.pdf_inspect.count_pdf_pages", return_value=None),
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "mineru"
        mock_sess.return_value.__aenter__.return_value = _mock_session_with_capture(adds)

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("weird.pdf", b"%PDF-1.4\ngarbage", "application/pdf")},
        )

    assert resp.status_code == 201, resp.text
    doc_rows = [o for o in adds if isinstance(o, Document)]
    assert doc_rows[0].page_count is None


@pytest.mark.asyncio
async def test_detail_endpoint_returns_page_count(client):
    """GET /documents/{id} surfaces Document.page_count to API callers."""
    fake_doc = MagicMock(spec=Document)
    fake_doc.id = "doc-1"
    fake_doc.kb_id = "kb-x"
    fake_doc.title = "deck.pptx"
    fake_doc.source = "deck.pptx"
    fake_doc.chunk_count = 0
    fake_doc.status = "completed"
    fake_doc.error_msg = ""
    fake_doc.created_at = datetime(2026, 5, 14, 12, 0, 0)
    fake_doc.content = "# deck\n"
    fake_doc.images = []
    fake_doc.image_status = "completed"
    fake_doc.page_count = 23

    with patch("api.routes.documents.get_session") as mock_sess:
        session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = fake_doc
        session.execute = AsyncMock(return_value=result)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.get("/v1/knowledgebases/kb-x/documents/doc-1")

    assert resp.status_code == 200, resp.text
    assert resp.json()["page_count"] == 23


@pytest.mark.asyncio
async def test_detail_endpoint_returns_null_when_unset(client):
    """Docs with no page_count (docx, html, pre-W5-? rows) serialize as null."""
    fake_doc = MagicMock(spec=Document)
    fake_doc.id = "doc-2"
    fake_doc.kb_id = "kb-x"
    fake_doc.title = "notes.docx"
    fake_doc.source = "notes.docx"
    fake_doc.chunk_count = 0
    fake_doc.status = "completed"
    fake_doc.error_msg = ""
    fake_doc.created_at = datetime(2026, 5, 14, 12, 0, 0)
    fake_doc.content = "body"
    fake_doc.images = []
    fake_doc.image_status = "none"
    fake_doc.page_count = None

    with patch("api.routes.documents.get_session") as mock_sess:
        session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = fake_doc
        session.execute = AsyncMock(return_value=result)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.get("/v1/knowledgebases/kb-x/documents/doc-2")

    assert resp.status_code == 200, resp.text
    assert resp.json()["page_count"] is None
