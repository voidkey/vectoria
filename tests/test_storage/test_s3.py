import pytest
from unittest.mock import AsyncMock, patch

from storage.s3 import S3ObjectStorage


class _async_ctx:
    """Helper to make a mock work as an async context manager."""
    def __init__(self, mock):
        self._mock = mock
    async def __aenter__(self):
        return self._mock
    async def __aexit__(self, *args):
        pass


@pytest.fixture
def storage():
    return S3ObjectStorage(
        endpoint="http://localhost:9000",
        region="",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="test-bucket",
        addressing_style="path",
        presign_expires=3600,
    )


@pytest.mark.asyncio
async def test_put(storage):
    mock_client = AsyncMock()
    with patch.object(storage, "_client", return_value=_async_ctx(mock_client)):
        await storage.put("test/key.txt", b"hello", content_type="text/plain")
    mock_client.put_object.assert_called_once_with(
        Bucket="test-bucket", Key="test/key.txt", Body=b"hello", ContentType="text/plain",
    )


@pytest.mark.asyncio
async def test_put_no_content_type(storage):
    mock_client = AsyncMock()
    with patch.object(storage, "_client", return_value=_async_ctx(mock_client)):
        await storage.put("test/key.txt", b"hello")
    mock_client.put_object.assert_called_once_with(
        Bucket="test-bucket", Key="test/key.txt", Body=b"hello",
    )


@pytest.mark.asyncio
async def test_get(storage):
    body_mock = AsyncMock()
    body_mock.read = AsyncMock(return_value=b"content")
    mock_client = AsyncMock()
    mock_client.get_object = AsyncMock(return_value={"Body": body_mock})

    with patch.object(storage, "_client", return_value=_async_ctx(mock_client)):
        result = await storage.get("test/key.txt")
    assert result == b"content"


@pytest.mark.asyncio
async def test_delete(storage):
    mock_client = AsyncMock()
    with patch.object(storage, "_client", return_value=_async_ctx(mock_client)):
        await storage.delete("test/key.txt")
    mock_client.delete_object.assert_called_once_with(Bucket="test-bucket", Key="test/key.txt")


@pytest.mark.asyncio
async def test_presign_url_default_expires(storage):
    mock_client = AsyncMock()
    mock_client.generate_presigned_url = AsyncMock(return_value="https://signed-url")
    with patch.object(storage, "_client", return_value=_async_ctx(mock_client)):
        url = await storage.presign_url("test/key.txt")
    assert url == "https://signed-url"
    mock_client.generate_presigned_url.assert_called_once_with(
        "get_object",
        Params={"Bucket": "test-bucket", "Key": "test/key.txt"},
        ExpiresIn=3600,
    )


@pytest.mark.asyncio
async def test_presign_url_custom_expires(storage):
    mock_client = AsyncMock()
    mock_client.generate_presigned_url = AsyncMock(return_value="https://signed-url")
    with patch.object(storage, "_client", return_value=_async_ctx(mock_client)):
        url = await storage.presign_url("test/key.txt", expires=600)
    mock_client.generate_presigned_url.assert_called_once_with(
        "get_object",
        Params={"Bucket": "test-bucket", "Key": "test/key.txt"},
        ExpiresIn=600,
    )


@pytest.mark.asyncio
async def test_exists_true(storage):
    mock_client = AsyncMock()
    with patch.object(storage, "_client", return_value=_async_ctx(mock_client)):
        result = await storage.exists("test/key.txt")
    assert result is True
