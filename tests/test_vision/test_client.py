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
        result = await client.describe(b"\x89PNG\r\n\x1a\n" + b"\x00" * 20)

    assert result == "A flowchart showing system architecture"
    mock_client.chat.completions.create.assert_called_once()


@pytest.mark.asyncio
async def test_describe_returns_empty_on_error():
    mock_client = AsyncMock()
    mock_client.chat.completions.create = AsyncMock(side_effect=Exception("API error"))

    with patch("vision.client.AsyncOpenAI", return_value=mock_client):
        client = VisionClient(base_url="http://test", api_key="test-key", model="gpt-4o")
        result = await client.describe(b"\x89PNG\r\n\x1a\n" + b"\x00" * 20)

    assert result == ""


@pytest.mark.asyncio
async def test_not_configured_returns_none():
    """When base_url is empty, client should not be created."""
    client = VisionClient(base_url="", api_key="", model="gpt-4o")
    assert not client.is_configured


def _api_error(cls, status: int, message: str = "boom"):
    import httpx
    request = httpx.Request("POST", "http://test/chat/completions")
    return cls(message, response=httpx.Response(status, request=request), body=None)


def _ok(content: str):
    return MagicMock(choices=[MagicMock(message=MagicMock(content=content))])


_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 20


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [
    _api_error(__import__("openai").NotFoundError, 404, "Model not exist."),
    _api_error(__import__("openai").BadRequestError, 400, "model [x] is offline"),
    _api_error(__import__("openai").RateLimitError, 429),
    _api_error(__import__("openai").InternalServerError, 500, "Internal error"),
])
async def test_falls_back_when_primary_model_unavailable(exc):
    from infra.circuit_breaker import get_breaker
    create = AsyncMock(side_effect=[exc, _ok("fallback answer")])
    mock_client = AsyncMock()
    mock_client.chat.completions.create = create

    with patch("vision.client.AsyncOpenAI", return_value=mock_client):
        client = VisionClient(base_url="http://test", api_key="k",
                              model="primary", fallback_model="backup")
        assert await client.describe(_PNG) == "fallback answer"

    assert [c.kwargs["model"] for c in create.call_args_list] == ["primary", "backup"]
    # A fallback success is a success for the breaker.
    assert get_breaker("vision")._consecutive_failures == 0


@pytest.mark.asyncio
async def test_no_fallback_for_request_errors():
    """A 400 that isn't about the model (e.g. bad image) would fail on
    any model — don't spend a second call on it."""
    import openai
    create = AsyncMock(side_effect=_api_error(openai.BadRequestError, 400, "invalid image"))
    mock_client = AsyncMock()
    mock_client.chat.completions.create = create

    with patch("vision.client.AsyncOpenAI", return_value=mock_client):
        client = VisionClient(base_url="http://test", api_key="k",
                              model="primary", fallback_model="backup")
        assert await client.describe(_PNG) == ""

    create.assert_called_once()


@pytest.mark.asyncio
async def test_parse_image_raises_when_fallback_also_fails():
    import openai
    create = AsyncMock(side_effect=[
        _api_error(openai.NotFoundError, 404),
        _api_error(openai.InternalServerError, 500),
    ])
    mock_client = AsyncMock()
    mock_client.chat.completions.create = create

    with patch("vision.client.AsyncOpenAI", return_value=mock_client):
        client = VisionClient(base_url="http://test", api_key="k",
                              model="primary", fallback_model="backup")
        with pytest.raises(openai.InternalServerError):
            await client.parse_image(_PNG)

    assert create.call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("disable, expected", [(True, {"enable_thinking": False}), (False, None)])
async def test_disable_thinking_sets_extra_body(disable, expected):
    create = AsyncMock(return_value=_ok("ok"))
    mock_client = AsyncMock()
    mock_client.chat.completions.create = create

    with patch("vision.client.AsyncOpenAI", return_value=mock_client):
        client = VisionClient(base_url="http://test", api_key="k",
                              model="m", disable_thinking=disable)
        await client.describe(_PNG)

    assert create.call_args.kwargs.get("extra_body") == expected
