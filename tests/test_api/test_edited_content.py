"""Edited renditions: PUT/POST/GET/DELETE /documents/{id}/edited.

The contract these pin down:

  * the edited body goes to object storage under a per-revision key and
    ``documents.content`` is never touched (orthogonality is the reason
    this design needs no parse-side revision guard);
  * ``edited_revision`` is claimed atomically before the upload, so
    concurrent writers can't collide on a key;
  * the pointer update is guarded on the claimed revision, so a slow
    writer can't revert the document to older bytes;
  * caller-supplied filenames can't escape the document's prefix.
"""
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from db.models import Document


def _fake_doc(**overrides) -> MagicMock:
    doc = MagicMock(spec=Document)
    doc.id = "doc-1"
    doc.kb_id = "kb-x"
    doc.kind = "document"
    doc.title = "report.pdf"
    doc.source = "report.pdf"
    doc.content = "# original parse output"
    doc.chunk_count = 3
    doc.status = "completed"
    doc.index_status = "completed"
    doc.error_msg = ""
    doc.error_code = None
    doc.image_status = "completed"
    doc.page_count = None
    doc.images = []
    doc.created_at = datetime(2026, 8, 25, 9, 0, 0)
    doc.edited_storage_key = None
    doc.edited_revision = 0
    doc.edited_at = None
    for k, v in overrides.items():
        setattr(doc, k, v)
    return doc


class _SessionRecorder:
    """Stands in for ``get_session`` across the multi-session write path.

    The write path opens three sessions in sequence (load → claim →
    publish), so a single canned return value isn't enough: ``scalar``
    must yield the claimed revision and ``execute`` must yield the doc.
    Statements are recorded so tests can assert on the guarded UPDATE.
    """

    def __init__(self, doc, *, claimed_revision=1):
        self.doc = doc
        self.claimed_revision = claimed_revision
        self.statements: list = []

    def __call__(self, *_args, **_kwargs):
        session = AsyncMock()

        def _execute(stmt, *a, **kw):
            self.statements.append(stmt)
            result = MagicMock()
            result.scalar_one_or_none.return_value = self.doc
            return result

        async def _scalar(stmt, *a, **kw):
            self.statements.append(stmt)
            return self.claimed_revision

        session.execute = AsyncMock(side_effect=_execute)
        session.scalar = AsyncMock(side_effect=_scalar)
        session.commit = AsyncMock()
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=session)
        ctx.__aexit__ = AsyncMock(return_value=False)
        return ctx


def _patched(recorder, storage):
    return (
        patch("api.routes.edited.get_session", new=recorder),
        patch("api.routes.edited.get_storage", return_value=storage),
    )


def _mock_storage() -> AsyncMock:
    storage = AsyncMock()
    storage.presign_url = AsyncMock(return_value="https://signed/edit")
    return storage


# --------------------------------------------------------------------------
# PUT .../edited  (text form)
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_stores_markdown_under_revision_key(client):
    doc = _fake_doc()
    recorder = _SessionRecorder(doc, claimed_revision=1)
    storage = _mock_storage()
    p1, p2 = _patched(recorder, storage)

    with p1, p2:
        resp = await client.put(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited",
            json={"content": "# cleaned up\n\nmuch better"},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["revision"] == 1
    assert body["object_key"] == "edits/kb-x/doc-1/1/content.md"
    assert body["filename"] == "content.md"
    assert body["url"] == "https://signed/edit"
    assert body["edited_at"] is not None

    key, raw = storage.put.await_args.args
    assert key == "edits/kb-x/doc-1/1/content.md"
    assert raw == b"# cleaned up\n\nmuch better"
    assert "markdown" in storage.put.await_args.kwargs["content_type"]


@pytest.mark.asyncio
async def test_put_does_not_touch_parsed_content(client):
    """The whole design rests on this: an edit never writes ``content``.

    If it did, a re-parse (or the retry_dead_docs reaper) and an edit
    would race for the same field and we'd need a revision guard in
    ``handle_parse_document``.
    """
    doc = _fake_doc()
    recorder = _SessionRecorder(doc)
    p1, p2 = _patched(recorder, _mock_storage())

    with p1, p2:
        resp = await client.put(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited",
            json={"content": "totally different text"},
        )

    assert resp.status_code == 200, resp.text
    assert doc.content == "# original parse output"

    # No statement this route issues may write the content column.
    from sqlalchemy.sql.dml import Update
    written = set()
    for stmt in recorder.statements:
        if isinstance(stmt, Update):
            written |= {c.name for c in stmt.compile().statement._values}
    assert written == {"edited_revision", "edited_storage_key", "edited_at"}
    assert "content" not in written


@pytest.mark.asyncio
async def test_put_claims_revision_before_upload(client):
    """Key uniqueness under concurrency depends on this ordering."""
    doc = _fake_doc(edited_revision=4)
    recorder = _SessionRecorder(doc, claimed_revision=5)
    storage = _mock_storage()
    p1, p2 = _patched(recorder, storage)

    order: list[str] = []
    original_scalar_marker = recorder.claimed_revision

    async def _put(key, data, content_type=""):
        order.append(f"put:{key}")

    storage.put = AsyncMock(side_effect=_put)

    with p1, p2:
        resp = await client.put(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited",
            json={"content": "v5"},
        )

    assert resp.status_code == 200, resp.text
    assert resp.json()["revision"] == original_scalar_marker
    # The upload used the *claimed* revision, not the stale one read
    # during validation — otherwise two writers would share a key.
    assert order == ["put:edits/kb-x/doc-1/5/content.md"]


@pytest.mark.asyncio
async def test_put_publish_is_guarded_on_claimed_revision(client):
    doc = _fake_doc()
    recorder = _SessionRecorder(doc, claimed_revision=7)
    p1, p2 = _patched(recorder, _mock_storage())

    with p1, p2:
        resp = await client.put(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited",
            json={"content": "body"},
        )

    assert resp.status_code == 200, resp.text
    from sqlalchemy.sql.dml import Update
    publish = [
        s for s in recorder.statements
        if isinstance(s, Update)
        and "edited_storage_key" in {c.name for c in s.compile().statement._values}
    ]
    assert len(publish) == 1, f"expected one publish UPDATE, got {publish}"
    # Without this predicate a writer that claimed an older revision but
    # finished last would revert the document to stale bytes.
    assert "documents.edited_revision = " in str(publish[0].whereclause)


@pytest.mark.asyncio
async def test_put_rejects_blank_content(client):
    doc = _fake_doc()
    p1, p2 = _patched(_SessionRecorder(doc), _mock_storage())

    with p1, p2:
        resp = await client.put(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited",
            json={"content": "   \n\t  "},
        )

    assert resp.status_code == 400
    assert resp.json()["error_code"] == 1202  # EMPTY_CONTENT


@pytest.mark.asyncio
async def test_put_rejects_oversize_content(client):
    from config import get_settings

    doc = _fake_doc()
    p1, p2 = _patched(_SessionRecorder(doc), _mock_storage())
    cfg = get_settings()

    with p1, p2, patch.object(cfg, "max_content_chars", 10):
        resp = await client.put(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited",
            json={"content": "x" * 11},
        )

    assert resp.status_code == 413
    body = resp.json()
    assert body["error_code"] == 1203  # CONTENT_TOO_LARGE
    assert body["error_data"] == {"current": 11, "limit": 10}


@pytest.mark.asyncio
async def test_put_404_for_missing_document(client):
    recorder = _SessionRecorder(None)
    p1, p2 = _patched(recorder, _mock_storage())

    with p1, p2:
        resp = await client.put(
            "/v1/knowledgebases/kb-x/documents/nope/edited",
            json={"content": "hi"},
        )

    assert resp.status_code == 404
    assert resp.json()["error_code"] == 1301  # NOT_FOUND


@pytest.mark.asyncio
async def test_put_rejects_site_capture(client):
    doc = _fake_doc(kind="site_capture")
    p1, p2 = _patched(_SessionRecorder(doc), _mock_storage())

    with p1, p2:
        resp = await client.put(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited",
            json={"content": "hi"},
        )

    assert resp.status_code == 400
    assert resp.json()["error_code"] == 1218  # EDIT_NOT_SUPPORTED


@pytest.mark.asyncio
async def test_put_stale_base_revision_conflicts(client):
    """The atomic bump's WHERE clause fails → no row returned → 409."""
    doc = _fake_doc(edited_revision=3)
    recorder = _SessionRecorder(doc, claimed_revision=None)
    storage = _mock_storage()
    p1, p2 = _patched(recorder, storage)

    with p1, p2:
        resp = await client.put(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited",
            json={"content": "hi", "base_revision": 1},
        )

    assert resp.status_code == 409
    assert resp.json()["error_code"] == 1219  # EDIT_REVISION_CONFLICT
    assert resp.json()["retryable"] is True
    # A rejected claim must not leave bytes behind in storage.
    storage.put.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("evil", [
    "../../../etc/passwd",
    "/absolute/escape.md",
    "..",
    "sub/dir/name.md",
    "..\\..\\windows.md",
])
async def test_put_filename_cannot_escape_document_prefix(client, evil):
    doc = _fake_doc()
    recorder = _SessionRecorder(doc, claimed_revision=1)
    storage = _mock_storage()
    p1, p2 = _patched(recorder, storage)

    with p1, p2:
        resp = await client.put(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited",
            json={"content": "hi", "filename": evil},
        )

    assert resp.status_code == 200, resp.text
    key = resp.json()["object_key"]
    assert key.startswith("edits/kb-x/doc-1/1/")
    # Exactly one segment after the revision — no traversal, no nesting.
    assert key.count("/") == 4
    assert ".." not in key


@pytest.mark.asyncio
async def test_put_keeps_caller_filename(client):
    doc = _fake_doc()
    p1, p2 = _patched(_SessionRecorder(doc, claimed_revision=2), _mock_storage())

    with p1, p2:
        resp = await client.put(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited",
            json={"content": "hi", "filename": "cleaned-report.md"},
        )

    assert resp.status_code == 200, resp.text
    assert resp.json()["object_key"] == "edits/kb-x/doc-1/2/cleaned-report.md"
    assert resp.json()["filename"] == "cleaned-report.md"


# --------------------------------------------------------------------------
# POST .../edited/file
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upload_file_stores_bytes_verbatim(client):
    doc = _fake_doc()
    recorder = _SessionRecorder(doc, claimed_revision=1)
    storage = _mock_storage()
    p1, p2 = _patched(recorder, storage)

    payload = b"PK\x03\x04binary-docx-ish"
    with p1, p2:
        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited/file",
            files={"file": ("cleaned.docx", payload, "application/vnd.ms-word")},
        )

    assert resp.status_code == 200, resp.text
    assert resp.json()["object_key"] == "edits/kb-x/doc-1/1/cleaned.docx"
    key, raw = storage.put.await_args.args
    assert raw == payload


@pytest.mark.asyncio
async def test_upload_file_rejects_empty(client):
    doc = _fake_doc()
    p1, p2 = _patched(_SessionRecorder(doc), _mock_storage())

    with p1, p2:
        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited/file",
            files={"file": ("empty.md", b"", "text/markdown")},
        )

    assert resp.status_code == 400
    assert resp.json()["error_code"] == 1210  # EMPTY_UPLOAD


@pytest.mark.asyncio
async def test_upload_file_rejects_oversize(client):
    from config import get_settings

    doc = _fake_doc()
    p1, p2 = _patched(_SessionRecorder(doc), _mock_storage())
    cfg = get_settings()

    with p1, p2, patch.object(cfg, "max_upload_bytes", 4):
        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited/file",
            files={"file": ("big.md", b"aaaaaaaa", "text/markdown")},
        )

    assert resp.status_code == 413
    assert resp.json()["error_code"] == 1204  # UPLOAD_TOO_LARGE


@pytest.mark.asyncio
async def test_upload_file_honours_base_revision_query(client):
    doc = _fake_doc(edited_revision=2)
    recorder = _SessionRecorder(doc, claimed_revision=None)
    storage = _mock_storage()
    p1, p2 = _patched(recorder, storage)

    with p1, p2:
        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited/file?base_revision=0",
            files={"file": ("x.md", b"hello", "text/markdown")},
        )

    assert resp.status_code == 409
    storage.put.assert_not_awaited()


# --------------------------------------------------------------------------
# GET .../edited
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_returns_metadata_and_presigned_url(client):
    doc = _fake_doc(
        edited_storage_key="edits/kb-x/doc-1/3/cleaned.md",
        edited_revision=3,
        edited_at=datetime(2026, 8, 25, 10, 30, 0),
    )
    p1, p2 = _patched(_SessionRecorder(doc), _mock_storage())

    with p1, p2:
        resp = await client.get(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["revision"] == 3
    assert body["filename"] == "cleaned.md"
    assert body["object_key"] == "edits/kb-x/doc-1/3/cleaned.md"
    assert body["url"] == "https://signed/edit"
    assert body["edited_at"] == "2026-08-25T10:30:00"
    # Not fetched unless asked for.
    assert body["content"] is None


@pytest.mark.asyncio
async def test_get_inlines_content_when_requested(client):
    doc = _fake_doc(
        edited_storage_key="edits/kb-x/doc-1/1/content.md", edited_revision=1)
    storage = _mock_storage()
    storage.get = AsyncMock(return_value="# cleaned 中文".encode("utf-8"))
    p1, p2 = _patched(_SessionRecorder(doc), storage)

    with p1, p2:
        resp = await client.get(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited?include_content=true")

    assert resp.status_code == 200, resp.text
    assert resp.json()["content"] == "# cleaned 中文"


@pytest.mark.asyncio
async def test_get_binary_artifact_returns_null_content_not_500(client):
    doc = _fake_doc(
        edited_storage_key="edits/kb-x/doc-1/1/cleaned.docx", edited_revision=1)
    storage = _mock_storage()
    storage.get = AsyncMock(return_value=b"\xff\xfe\x00binary")
    p1, p2 = _patched(_SessionRecorder(doc), storage)

    with p1, p2:
        resp = await client.get(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited?include_content=true")

    assert resp.status_code == 200, resp.text
    assert resp.json()["content"] is None
    assert resp.json()["url"] == "https://signed/edit"


@pytest.mark.asyncio
async def test_get_404_when_never_edited(client):
    doc = _fake_doc()  # edited_storage_key is None
    p1, p2 = _patched(_SessionRecorder(doc), _mock_storage())

    with p1, p2:
        resp = await client.get(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited")

    assert resp.status_code == 404
    assert resp.json()["error_code"] == 1217  # EDIT_NOT_FOUND


# --------------------------------------------------------------------------
# DELETE .../edited
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_unlinks_but_keeps_revision_counter(client):
    doc = _fake_doc(
        edited_storage_key="edits/kb-x/doc-1/2/content.md", edited_revision=2)
    recorder = _SessionRecorder(doc)
    storage = _mock_storage()
    p1, p2 = _patched(recorder, storage)

    with p1, p2:
        resp = await client.delete(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited")

    assert resp.status_code == 204
    rendered = " ".join(str(s) for s in recorder.statements)
    assert "edited_storage_key" in rendered
    # The counter must survive: resetting it would let the next write
    # reuse the withdrawn revision's object key.
    assert "edited_revision=" not in rendered.replace(" ", "")
    # Withdrawal is an unlink, not a purge — objects stay recoverable.
    storage.delete.assert_not_awaited()
    storage.delete_prefix.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_404_when_never_edited(client):
    doc = _fake_doc()
    p1, p2 = _patched(_SessionRecorder(doc), _mock_storage())

    with p1, p2:
        resp = await client.delete(
            "/v1/knowledgebases/kb-x/documents/doc-1/edited")

    assert resp.status_code == 404
    assert resp.json()["error_code"] == 1217


# --------------------------------------------------------------------------
# Cross-cutting: document read paths + lifecycle
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_detail_endpoint_surfaces_edit_state(client):
    doc = _fake_doc(
        edited_storage_key="edits/kb-x/doc-1/2/content.md",
        edited_revision=2,
        edited_at=datetime(2026, 8, 25, 11, 0, 0),
    )

    with patch("api.routes.documents.get_session") as mock_sess:
        session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = doc
        session.execute = AsyncMock(return_value=result)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.get("/v1/knowledgebases/kb-x/documents/doc-1")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["has_edit"] is True
    assert body["edited_revision"] == 2
    assert body["edited_at"] == "2026-08-25T11:00:00"
    # The detail payload still carries the *parsed* content, not the edit.
    assert body["content"] == "# original parse output"


@pytest.mark.asyncio
async def test_detail_endpoint_defaults_for_unedited_document(client):
    doc = _fake_doc()

    with patch("api.routes.documents.get_session") as mock_sess:
        session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = doc
        session.execute = AsyncMock(return_value=result)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.get("/v1/knowledgebases/kb-x/documents/doc-1")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["has_edit"] is False
    assert body["edited_revision"] == 0
    assert body["edited_at"] is None


@pytest.mark.asyncio
async def test_withdrawn_edit_reports_has_edit_false_despite_counter(client):
    """``edited_revision`` is monotonic — it is not a "has an edit" signal."""
    doc = _fake_doc(edited_storage_key=None, edited_revision=5, edited_at=None)

    with patch("api.routes.documents.get_session") as mock_sess:
        session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = doc
        session.execute = AsyncMock(return_value=result)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.get("/v1/knowledgebases/kb-x/documents/doc-1")

    assert resp.status_code == 200, resp.text
    assert resp.json()["has_edit"] is False
    assert resp.json()["edited_revision"] == 5


@pytest.mark.asyncio
async def test_document_delete_reclaims_edit_objects(client):
    """Withdrawn/superseded revisions are only reclaimed here, so this
    prefix delete is the sole thing standing between us and orphaned
    objects accumulating in the bucket."""
    doc = _fake_doc()
    storage = AsyncMock()

    with (
        patch("api.routes.documents.get_session") as mock_sess,
        patch("api.routes.documents.get_storage", return_value=storage),
        patch("api.routes.documents.PgVectorStore") as mock_store,
    ):
        session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = doc
        session.execute = AsyncMock(return_value=result)
        session.delete = AsyncMock()
        mock_sess.return_value.__aenter__.return_value = session
        store_ctx = AsyncMock()
        store_ctx.__aenter__ = AsyncMock(return_value=AsyncMock())
        store_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_store.create = AsyncMock(return_value=store_ctx)

        resp = await client.delete("/v1/knowledgebases/kb-x/documents/doc-1")

    assert resp.status_code == 204, resp.text
    prefixes = [c.args[0] for c in storage.delete_prefix.await_args_list]
    assert "edits/kb-x/doc-1/" in prefixes
    assert "images/kb-x/doc-1/" in prefixes
