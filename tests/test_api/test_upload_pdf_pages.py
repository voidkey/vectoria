"""PDF page-count gate at the upload entry.

Lives alongside the byte-size and MIME gates because all three exist
to keep the heavy parse path from ever seeing a problematic file.
The byte cap doesn't catch "small file, many pages" — a 19 MB scanned
PDF can hide 1000+ pages that mineru cannot OCR within its 120 s
timeout, burning 3 × retries of GPU time before fallback.
"""
import pytest
from datetime import datetime
from unittest.mock import patch, AsyncMock, MagicMock


@pytest.mark.asyncio
async def test_pdf_with_too_many_pages_returns_413(client):
    """A PDF whose page count exceeds the policy is rejected before
    storage write, with the dedicated error code so clients can
    tell it apart from a byte-cap rejection.
    """
    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        # Mock the page count rather than constructing a 1000-page PDF
        # — the unit suite covers count_pdf_pages on real bytes.
        patch("api.pdf_inspect.count_pdf_pages", return_value=999),
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "mineru"

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("huge.pdf", b"%PDF-1.4\nfake bytes", "application/pdf")},
        )

    assert resp.status_code == 413, resp.text
    assert resp.json()["code"] == 1208  # PDF_TOO_MANY_PAGES
    # No S3 write — the whole point of the gate is to reject before
    # spending bandwidth on a doc the worker can't process anyway.
    mock_storage.return_value.put.assert_not_called()


@pytest.mark.asyncio
async def test_pdf_under_limit_passes_the_gate(client):
    """A PDF whose page count is within policy proceeds to enqueue.
    Sanity: the gate does not false-positive on small docs.
    """
    from db.models import Task
    added: list[object] = []

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("api.pdf_inspect.count_pdf_pages", return_value=10),
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "mineru"

        session = AsyncMock()
        miss = MagicMock()
        miss.scalar_one_or_none.return_value = None
        session.execute = AsyncMock(return_value=miss)

        def _refresh(obj):
            obj.created_at = datetime(2026, 4, 30, 12, 0, 0)
        session.add = MagicMock(side_effect=lambda obj: added.append(obj))
        session.commit = AsyncMock()
        session.refresh = AsyncMock(side_effect=_refresh)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("ok.pdf", b"%PDF-1.4\nfake", "application/pdf")},
        )

    assert resp.status_code == 201, resp.text
    parse_tasks = [
        o for o in added if isinstance(o, Task) and o.task_type == "parse_document"
    ]
    assert len(parse_tasks) == 1


@pytest.mark.asyncio
async def test_non_pdf_skips_page_count_check(client):
    """The gate only fires for ``.pdf`` — uploading a docx/txt/etc.
    must not call count_pdf_pages, otherwise we'd waste a pypdfium2
    load on every non-PDF upload (and worse, fail-open garbage results
    could leak into rejection logs).
    """
    from db.models import Task
    added: list[object] = []

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("api.pdf_inspect.count_pdf_pages") as mock_count,
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "markitdown"

        session = AsyncMock()
        miss = MagicMock()
        miss.scalar_one_or_none.return_value = None
        session.execute = AsyncMock(return_value=miss)

        def _refresh(obj):
            obj.created_at = datetime(2026, 4, 30, 12, 0, 0)
        session.add = MagicMock(side_effect=lambda obj: added.append(obj))
        session.commit = AsyncMock()
        session.refresh = AsyncMock(side_effect=_refresh)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("notes.txt", b"hello world", "text/plain")},
        )

    assert resp.status_code == 201, resp.text
    mock_count.assert_not_called()


@pytest.mark.asyncio
async def test_corrupt_pdf_falls_through_gate(client):
    """count_pdf_pages returns ``None`` for unloadable bytes — the gate
    must treat that as "can't tell, let downstream decide" rather than
    rejecting. Otherwise a pypdfium2 hiccup would block valid uploads
    that mineru could still parse via its own pipeline.
    """
    from db.models import Task
    added: list[object] = []

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("api.pdf_inspect.count_pdf_pages", return_value=None),
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "mineru"

        session = AsyncMock()
        miss = MagicMock()
        miss.scalar_one_or_none.return_value = None
        session.execute = AsyncMock(return_value=miss)

        def _refresh(obj):
            obj.created_at = datetime(2026, 4, 30, 12, 0, 0)
        session.add = MagicMock(side_effect=lambda obj: added.append(obj))
        session.commit = AsyncMock()
        session.refresh = AsyncMock(side_effect=_refresh)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("weird.pdf", b"%PDF-1.4\ngarbage", "application/pdf")},
        )

    assert resp.status_code == 201, resp.text
    parse_tasks = [
        o for o in added if isinstance(o, Task) and o.task_type == "parse_document"
    ]
    assert len(parse_tasks) == 1
