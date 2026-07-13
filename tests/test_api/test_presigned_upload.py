"""Presigned direct-upload path: mint URL (`uploads`) + `complete`."""
import base64
import pytest
from unittest.mock import patch, AsyncMock


def test_upload_not_found_error_code_exists():
    from api.errors import ErrorCode
    assert ErrorCode.UPLOAD_NOT_FOUND == 1211


def test_presign_upload_expires_default():
    from config import get_settings
    assert get_settings().s3_presign_upload_expires == 600


def test_upload_id_roundtrip():
    from api.routes.documents import _encode_upload_id, _decode_upload_id
    key = "upload_staging/kb-x/doc-1/report file.pdf"
    token = _encode_upload_id(key)
    assert "/" not in token  # single URL path segment
    assert _decode_upload_id(token) == key


def test_decode_bad_token_raises():
    from api.routes.documents import _decode_upload_id
    with pytest.raises(Exception):
        _decode_upload_id("!!!not-base64!!!")


@pytest.mark.asyncio
async def test_uploads_mints_url(client):
    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents._find_existing_by_hash", new=AsyncMock(return_value=None)),
    ):
        mock_storage.return_value = AsyncMock()
        mock_storage.return_value.presign_put_url = AsyncMock(return_value="https://s3/put?sig=1")

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/uploads",
            json={"filename": "report.pdf"},
        )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["upload_url"] == "https://s3/put?sig=1"
    assert body["method"] == "PUT"
    assert body["dedup_hit"] is False
    from api.routes.documents import _decode_upload_id
    assert _decode_upload_id(body["upload_id"]).startswith("upload_staging/kb-x/")


@pytest.mark.asyncio
async def test_uploads_prededup_hit_skips_url(client):
    existing = AsyncMock()
    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents._find_existing_by_hash", new=AsyncMock(return_value=existing)),
        patch("api.routes.documents._dedup_response") as mock_dedup,
    ):
        from api.schemas import DocumentIngestResponse
        mock_dedup.return_value = DocumentIngestResponse(
            id="d1", kb_id="kb-x", title="t", source="s", chunk_count=0,
            status="completed", index_status="completed", error_msg="",
            created_at="2026-07-13T00:00:00",
        )
        mock_storage.return_value = AsyncMock()

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/uploads",
            json={"filename": "report.pdf", "sha256": "abc"},
        )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["dedup_hit"] is True
    assert body["document"]["id"] == "d1"
    assert body.get("upload_url") is None
    mock_storage.return_value.presign_put_url.assert_not_called()


@pytest.mark.asyncio
async def test_uploads_size_early_reject(client):
    from config import get_settings
    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
    ):
        mock_storage.return_value = AsyncMock()
        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/uploads",
            json={"filename": "big.pdf", "size": get_settings().max_upload_bytes + 1},
        )
    assert resp.status_code == 413, resp.text
    assert resp.json()["code"] == 1204  # UPLOAD_TOO_LARGE
    mock_storage.return_value.presign_put_url.assert_not_called()


@pytest.mark.asyncio
async def test_uploads_501_when_backend_cannot_presign(client):
    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents._find_existing_by_hash", new=AsyncMock(return_value=None)),
    ):
        mock_storage.return_value = AsyncMock()
        mock_storage.return_value.presign_put_url = AsyncMock(side_effect=NotImplementedError)
        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/uploads",
            json={"filename": "report.pdf"},
        )
    assert resp.status_code == 501, resp.text
    assert resp.json()["code"] == 1212  # UPLOAD_NOT_SUPPORTED
