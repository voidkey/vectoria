import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from vision.client import VisionClient


@pytest.mark.asyncio
async def test_describe_image_returns_description():
    mock_response = MagicMock()
    mock_response.choices = [MagicMock(message=MagicMock(content="A flowchart showing system architecture"))]

    mock_client = AsyncMock()
    mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

    with patch("vision.client.AsyncOpenAI", return_value=mock_client):
        client = VisionClient(base_url="http://test", api_key="test-key", model="gpt-4o")
        result = await client.describe(b"\x89PNG fake image bytes")

    assert result == "A flowchart showing system architecture"
    mock_client.chat.completions.create.assert_called_once()


@pytest.mark.asyncio
async def test_describe_returns_empty_on_error():
    mock_client = AsyncMock()
    mock_client.chat.completions.create = AsyncMock(side_effect=Exception("API error"))

    with patch("vision.client.AsyncOpenAI", return_value=mock_client):
        client = VisionClient(base_url="http://test", api_key="test-key", model="gpt-4o")
        result = await client.describe(b"\x89PNG fake image bytes")

    assert result == ""


@pytest.mark.asyncio
async def test_not_configured_returns_none():
    """When base_url is empty, client should not be created."""
    client = VisionClient(base_url="", api_key="", model="gpt-4o")
    assert not client.is_configured
