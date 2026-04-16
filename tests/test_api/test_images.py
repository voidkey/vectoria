import pytest
from unittest.mock import patch, AsyncMock, MagicMock


@pytest.mark.asyncio
async def test_get_images_returns_list(client):
    """Test that the images endpoint returns image metadata."""
    from db.models import DocumentImage

    mock_image = MagicMock(spec=DocumentImage)
    mock_image.id = "img-uuid"
    mock_image.storage_key = "images/kb/doc/img.png"
    mock_image.filename = "img.png"
    mock_image.width = 1200
    mock_image.height = 800
    mock_image.alt = "test image"
    mock_image.context = "Some context text"
    mock_image.section_title = "Architecture"
    mock_image.description = "A diagram"
    mock_image.vision_status = "completed"
    mock_image.image_index = 0

    mock_storage = AsyncMock()
    mock_storage.presign_url = AsyncMock(return_value="https://signed/img.png")

    mock_doc = MagicMock()
    mock_doc.id = "doc-uuid"
    mock_doc.kb_id = "kb-uuid"

    with (
        patch("api.routes.images.get_session") as mock_get_session,
        patch("api.routes.images.get_storage", return_value=mock_storage),
    ):
        mock_session = AsyncMock()
        # First query: document lookup
        mock_result1 = MagicMock()
        mock_result1.scalar_one_or_none.return_value = mock_doc
        # Second query: images lookup
        mock_result2 = MagicMock()
        mock_result2.scalars.return_value.all.return_value = [mock_image]

        mock_session.execute = AsyncMock(side_effect=[mock_result1, mock_result2])
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_get_session.return_value = mock_ctx

        resp = await client.get("/v1/knowledgebases/kb-uuid/documents/doc-uuid/images")

    assert resp.status_code == 200
    body = resp.json()
    assert body["doc_id"] == "doc-uuid"
    assert len(body["images"]) == 1
    img = body["images"][0]
    assert img["id"] == "img-uuid"
    assert img["url"] == "https://signed/img.png"
    assert img["filename"] == "img.png"
    assert img["width"] == 1200
    assert img["aspect_ratio"] == "3:2"
    assert img["description"] == "A diagram"
    assert img["vision_status"] == "completed"


@pytest.mark.asyncio
async def test_get_images_doc_not_found(client):
    with patch("api.routes.images.get_session") as mock_get_session:
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_get_session.return_value = mock_ctx

        resp = await client.get("/v1/knowledgebases/kb-uuid/documents/doc-uuid/images")

    assert resp.status_code == 404
