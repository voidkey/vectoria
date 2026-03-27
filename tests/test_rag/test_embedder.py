import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from rag.embedder import Embedder


@pytest.mark.asyncio
async def test_embed_returns_floats():
    mock_response = MagicMock()
    mock_response.data = [MagicMock(embedding=[0.1, 0.2, 0.3])]

    with patch("rag.embedder.AsyncOpenAI") as MockOAI:
        instance = MockOAI.return_value
        instance.embeddings.create = AsyncMock(return_value=mock_response)

        embedder = Embedder()
        result = await embedder.embed("hello world")

    assert result == [0.1, 0.2, 0.3]


@pytest.mark.asyncio
async def test_embed_batch():
    mock_response = MagicMock()
    mock_response.data = [
        MagicMock(embedding=[0.1, 0.2]),
        MagicMock(embedding=[0.3, 0.4]),
    ]

    with patch("rag.embedder.AsyncOpenAI") as MockOAI:
        instance = MockOAI.return_value
        instance.embeddings.create = AsyncMock(return_value=mock_response)

        embedder = Embedder()
        results = await embedder.embed_batch(["text1", "text2"])

    assert len(results) == 2
    assert results[0] == [0.1, 0.2]
