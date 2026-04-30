"""PPTX slide-count gate at the upload entry.

Mirrors test_upload_pdf_pages — same shape of attack (small file,
many slides multiplies parse + vision cost), so the gate sits next
to the PDF page-count gate and uses the same upload-reject plumbing.
"""
import pytest
from datetime import datetime
from unittest.mock import patch, AsyncMock, MagicMock


@pytest.mark.asyncio
async def test_pptx_with_too_many_slides_returns_413(client):
    """A PPTX whose slide count exceeds policy is rejected before
    storage write, with a distinct error code so clients can
    distinguish from the byte-cap (1204) and PDF-page (1208) gates.
    """
    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        patch("api.pptx_inspect.count_pptx_slides", return_value=999),
        # Skip MIME sniff — fake bytes won't pass magic-byte check.
        patch("api.mime_sniff.check_mime", return_value=(True, "office_zip")),
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "pptx"

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/file",
            files={"file": ("huge.pptx", b"PK\x03\x04 fake", "application/vnd.openxmlformats-officedocument.presentationml.presentation")},
        )

    assert resp.status_code == 413, resp.text
    assert resp.json()["code"] == 1209  # PPTX_TOO_MANY_SLIDES
    mock_storage.return_value.put.assert_not_called()


@pytest.mark.asyncio
async def test_pptx_under_limit_passes_the_gate(client):
    """A PPTX within policy proceeds to enqueue — sanity check that
    the gate doesn't false-positive on small decks.
    """
    from db.models import Task
    added: list[object] = []

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("api.pptx_inspect.count_pptx_slides", return_value=12),
        patch("api.mime_sniff.check_mime", return_value=(True, "office_zip")),
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "pptx"

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
            files={"file": ("ok.pptx", b"PK\x03\x04 fake", "application/vnd.openxmlformats-officedocument.presentationml.presentation")},
        )

    assert resp.status_code == 201, resp.text
    parse_tasks = [
        o for o in added if isinstance(o, Task) and o.task_type == "parse_document"
    ]
    assert len(parse_tasks) == 1


@pytest.mark.asyncio
async def test_non_pptx_skips_slide_count_check(client):
    """Only ``.pptx`` triggers the slide gate — uploading a docx/pdf/etc.
    must not call count_pptx_slides. Otherwise we'd pay a zip-open per
    upload and (worse) leak its fail-open None into rejection logs.
    """
    from db.models import Task
    added: list[object] = []

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("api.pptx_inspect.count_pptx_slides") as mock_count,
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
async def test_corrupt_pptx_falls_through_gate(client):
    """count_pptx_slides returns ``None`` on a bad zip — gate must
    fail-open so a zip-format hiccup doesn't block uploads the parser
    chain might still rescue (markitdown fallback can pull text from
    weird Office files mineru/python-pptx miss).
    """
    from db.models import Task
    added: list[object] = []

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.registry") as mock_registry,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("api.pptx_inspect.count_pptx_slides", return_value=None),
        patch("api.mime_sniff.check_mime", return_value=(True, "office_zip")),
    ):
        mock_storage.return_value = AsyncMock()
        mock_registry.auto_select.return_value = "pptx"

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
            files={"file": ("weird.pptx", b"PK\x03\x04 garbage", "application/vnd.openxmlformats-officedocument.presentationml.presentation")},
        )

    assert resp.status_code == 201, resp.text
    parse_tasks = [
        o for o in added if isinstance(o, Task) and o.task_type == "parse_document"
    ]
    assert len(parse_tasks) == 1
